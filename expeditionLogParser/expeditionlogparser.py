import csv
import getopt
import math
import os
import sys
import xlrd

from packaging import version


class PolarPoint:
    x = 0
    y = 0
    twa = 0
    tws = 0
    velocity = 0
    inferred = False

    def __init__(self, twa: int, tws: int, velocity: float, inferred: bool):
        self.twa = twa
        self.tws = tws
        self.velocity = velocity
        self.y = round(math.cos(math.radians(twa)) * velocity, 5)
        self.x = round(math.sin(math.radians(twa)) * velocity, 5)
        self.inferred = inferred


class Polars:
    name = "BoatType"
    twa_range = []
    tws_range = []
    polar_data = dict()  # [TWA][TWS][Value] Dict of TWA each one with a Dict of TWS at the prev TWA

    def get_polar_target(self, twa: int, tws: int):
        return self.polar_data[twa][tws].velocity

    def get_polar_closest_polar_target(self, twa: int, tws: int):
        twa = abs(twa)
        twa_closest = self.twa_range[min(range(len(self.twa_range)), key=lambda i: abs(self.twa_range[i] - twa))]
        tws_closest = self.tws_range[min(range(len(self.tws_range)), key=lambda i: abs(self.tws_range[i] - tws))]
        try:
            target = self.polar_data[twa_closest][tws_closest].velocity
        except KeyError:
            # Set to 0 if there is not a polar for some reason at this index. Can happen when polar files are partial
            target = 0
        return target

    def load_expedition_format(self, input_file: str):
        #  Blue-water and Expedition follow this format of TWA TWS1 BSP1 TWST2 BSP2 on single row
        print("Loading Polar File: " + input_file)

        with open(input_file, newline='') as f:
            line1 = f.readline().strip()
            if line1 != "!Expedition polar":
                print("File format not correct. Expedition expected line 1 to be: !Expedition polar")

            for line in f:
                line = line.strip()
                data = line.split('\t')

                tws = 0
                twa = 0
                for index, item in enumerate(data):
                    if index == 0:
                        tws = int(round(float(item)))
                        self.tws_range.append(tws)
                        continue

                    if index % 2 == 0:
                        bsp = float(item)
                        polar_point = PolarPoint(twa, int(tws), bsp, False)
                        self.polar_data[twa][tws] = polar_point

                    else:
                        twa = int(round(float(item)))
                        if twa not in self.twa_range:
                            self.twa_range.append(twa)
                            self.polar_data[twa] = dict()


class LogFilter:
    # This class is used to build up all the parameters used in processing the log file.
    # Convenient way to group and pass this dara along the code path.
    apply_filter: bool = False  # Apply the filter default is off
    drop_min_speed: int = 1  # Will drop samples below this boat speed
    drop_min_wind_angle: int = 10  # Will drop sample with wind outside -5 to 5 and 175-180 and -175 - 180
    drop_min_windspeed: int = 1  # Will drop samples with wind below this level

    subsample: int = 1  # Default to all lines no sub sampling.
    time_window: int = 60  # Default window size used for event detection samples are 1 per sec in most logs
    average_samples: int = 0  # Default is no averaging (not implemented yet!)
    convert_time = False  # Converts time in the log from xls epoc to string format y:m:d h:m:s

    def __init__(self, apply_filter=False, subsample=1, convert_time_format=False, average_samples=0, polar_data=None):
        self.apply_filter = apply_filter
        self.subsample = subsample
        self.average_samples = average_samples
        self.convert_time = convert_time_format
        self.polar_data = polar_data


def convert_time(timestamp):
    # Note about time formats. Expedition uses Xls epoc time.
    # Unix Epoc time is number of seconds since Jan 1 1970 fractions of seconds are fractions in float
    # MS Excel date time starts on Jan 1 1900 and counts forward where 1 unit is a day.
    # As a ref conversion in Excel to epoc value DATE(1970,1,1))*86400
    # As a ref Conversion in Tableau calc would be Datetime(value + INT(#30 December 1899"))

    # Convert from XLS format to string
    datetime_date = xlrd.xldate_as_datetime(float(timestamp), 0)
    return datetime_date.strftime("%m/%d/%Y %H:%M:%S")


def convert_float(s):
    try:
        i = float(s)
    except ValueError:
        i = ''
    return i


def process_files(log_input, output_file, columns_to_keep, log_filter):
    log_files = []
    log_dir = ""
    process_header = True
    if os.path.isdir(log_input):
        # Use all files in the directory and sort by modified date. This should mostly sort the date by time.
        # Possibly in the future reorder the data if it is out of sequence witch is possible.
        log_dir = log_input
        log_files += [each for each in os.listdir(log_input) if each.endswith('.csv')]
        log_files = sorted(log_files, key=lambda x: os.path.getmtime(os.path.join(log_dir, x)))
    else:
        # Single file was passed in just use it standalone no merging is done.
        log_files += log_input

    with open(output_file, 'w', newline='') as output_data_file:
        print('Directory: ' + log_dir)
        print(f'Files to process: {log_files}')
        for log in log_files:  # Process all the logs in the directory
            read_log(os.path.join(log_dir, log), output_data_file, columns_to_keep, process_header, log_filter)
            process_header = False


def read_log(log_file, output_file, columns_to_keep, process_header, log_filter):
    print('Processing: ' + log_file)
    with open(log_file, mode='r') as log_file:
        # Determine the version it is in the first few lines of the log file.
        # The version is in the first few lines of the file with !vMajor.Minor.*
        for i in range(5):
            version_string = log_file.readline()
            if '!v' in version_string:
                version_number = version.parse(version_string.strip().split('!v')[1])
            else:
                continue

            if version_number >= version.parse('11.16.0'):
                # v16 changed from std csv to sparse, very different.
                log_file.seek(0)
                read_log_v16(columns_to_keep, log_file, output_file, process_header, log_filter)
                break
            elif version_number >= version.parse('11.8.0'):
                # format seems the same as 15 just no ! on line 1? (Not sure which version resulted in this changed)
                # last version with true CSV is 15
                log_file.seek(0)
                read_log_v8(columns_to_keep, log_file, output_file, process_header, log_filter)
                break
            else:
                # The version is in the first few lines of the file with !vMajor.Minor.*
                if i >= 4:
                    print("Unknown file versions or formats! Reach out for help.")
                    raise Exception("Unknown file versions or formats!")


def read_log_v8(columns_to_keep, log_file, output_file, process_header, log_filter):
    # V8 is mostly a usable CSV format, so we will just clean up the header and merge files in the directory.
    # Read first 3 lines !Boat, !boat !v
    header = log_file.readline().strip()
    version_string = log_file.readline().strip()
    headers = []

    leg_name = os.path.basename(os.path.dirname(log_file.name))  # The directory will be used as the leg name

    # Build dict of Variables to keep and the Index to us for mapping. Utc=0 BSP=1 etc.
    if 'Boat' in header and version_string.startswith('!v'):
        headers = header.split(",")

    # Build CSV header
    data_writer = csv.DictWriter(output_file, fieldnames=columns_to_keep, delimiter=',',
                                 quotechar='"', quoting=csv.QUOTE_MINIMAL)

    # Only pass the header once for the first log. (Alt check the file size here?)
    if process_header:
        data_writer.writeheader()
        output_file.flush()

    counter = 1
    for line in log_file:
        if 'Boat' in line or '!v' in line:
            # Drop any new header lines assume we do not change version on the same day. (Simple solution for now)
            # print("Second header in file, expedition restarted on this day process as same version")
            continue

        # Process the data into CSV keeping only the columns asked for.
        # If items is missing use '' get is used to set the default.
        try:
            data = line.strip().split(",")
            data_pairs = dict(zip(headers, [convert_float(i) for i in data]))

            # Add extra calculated columns
            data_pairs["Leg_Name"] = leg_name
            data_pairs["Tack_Gybe_Detect"] = 0

            data_final = {key: data_pairs.get(key, '') for key in columns_to_keep}

            # --- Filter out lines if there is no instrument data present.

            if data_final['BSP'] == '' or data_final['Lat'] == '' or data_final['Lon'] == '':
                continue  # boat is on and recording but no instruments are enabled.

            if log_filter.apply_filter:
                if data_final['BSP'] <= log_filter.drop_min_speed:
                    continue

                if data_final['TWS'] <= log_filter.drop_min_windspeed:
                    continue

                if abs(data_final['TWA']) <= log_filter.drop_min_wind_angle or \
                        abs(data_final['TWA']) >= (180 - log_filter.drop_min_wind_angle):
                    continue

            # --- end of filter section

            if log_filter.convert_time:  # Convert Excel time to a string
                data_final['Utc'] = convert_time(data_final['Utc'])

            if counter >= log_filter.subsample:
                counter = 1
                data_writer.writerow(data_final)
            else:
                counter += 1

        except Exception as e:
            # Drop line as formatting is off simpler to drop the line than try and recover for now.
            # Could be a partial line or a line that had a value with no key pair.
            # Print any info to help with debug if code hits this area.
            print('Dropping a log line that is partial or not parsed correctly.')
            print('Log line :' + line)
            print(type(e))


def read_log_v16(columns_to_keep, log_file, output_file, process_header, log_filter):
    # V16 is not a true CSV it is sparse with the header in row 0 and index in row 1.
    # It does not have all entries on each row and so needs processing for other systems to use CSV.

    # Read first 3 lines !Boat, !boat !v
    header = log_file.readline().strip()
    index = log_file.readline().strip()
    version_string = log_file.readline().strip()
    header_index = {}

    leg_name = os.path.basename(os.path.dirname(log_file.name))  # The directory will be used as the leg name

    # Build dict of Variables to keep and the Index to us for mapping. Utc=0 BSP=1 etc.
    if header.startswith("!Boat") and index.startswith("!boat") and version_string.startswith("!v"):
        headers = dict(zip(header.split(","), index.split(",")))

        # Add calculated columns
        headers["Leg_Name"] = 'Leg_Name'
        headers["Leg_Name"] = 'Leg_Name'
        headers["Tack_Gybe_Detect"] = 'Tack_Gybe_Detect'
        headers["Target_BSP"] = 'Target_BSP'

        header_index = {headers[key]: key for key in columns_to_keep}

    # Build CSV header
    data_writer = csv.DictWriter(output_file, fieldnames=columns_to_keep, delimiter=',',
                                 quotechar='"', quoting=csv.QUOTE_MINIMAL)

    # Only pass the header once for the first log. (Alt check the file size here?)
    if process_header:
        data_writer.writeheader()
        output_file.flush()

    sub_counter = 1
    window_counter = 1
    data_window_current = []

    for line in log_file:
        if '!boat' in line or '!Boat' in line or '!v' in line:
            # Drop any new header lines assume we do not change version on the same day. (Simple solution for now)
            # print("Second header in file, expedition restarted on this day process as same version")
            continue

        # Process the data into CSV keeping only the ones we need.
        try:
            data = line.strip().split(",")
            index = 0
            data_to_keep = {}
            for i in data:
                if index % 2 == 0 and i.isdigit():
                    key = i
                    value = convert_float(data[index + 1])
                    # if the value is one to keep print it to the new true clean csv.
                    if key in header_index:
                        data_to_keep[header_index[key]] = value

                        if log_filter.convert_time and header_index[key] == "Utc":
                            data_to_keep[header_index[key]] = convert_time(value)
                index += 1

            # --- Filter out lines if there is no instrument data present.
            if 'Lat' not in data_to_keep or 'Lon' not in data_to_keep:
                continue  # boat is on and recording but no instruments are enabled gps is missing.
            if 'TWS' not in data_to_keep or 'TWA' not in data_to_keep:
                continue  # boat is on and recording but no instruments are enabled wind is missing.

            if log_filter.apply_filter:
                if data_to_keep['BSP'] <= log_filter.drop_min_speed:
                    continue

                if data_to_keep['TWS'] <= log_filter.drop_min_windspeed:
                    continue

                if abs(data_to_keep['TWA']) <= log_filter.drop_min_wind_angle or \
                        abs(data_to_keep['TWA']) >= (180 - log_filter.drop_min_wind_angle):
                    continue
            # --- end of filter section

            # --- Add calculated columns now
            if "Leg_Name" in header_index:
                data_to_keep["Leg_Name"] = leg_name

            if "Tack_Gybe_Detect" in header_index:
                data_to_keep["Tack_Gybe_Detect"] = 0

            if "Target_BSP" in header_index and log_filter.polar_data:
                data_to_keep["Target_BSP"] = log_filter.polar_data.get_polar_closest_polar_target(data_to_keep['TWA'],
                                                                                                  data_to_keep['TWS'])
            # --- end of calculated columns

            # Windowing ----- ProtoType

            if window_counter < log_filter.time_window:
                window_counter += 1
                if sub_counter >= log_filter.subsample:
                    sub_counter = 1
                    data_window_current.append(data_to_keep)
                else:
                    # Drop samples until the counter is hit every nth sample
                    sub_counter += 1
            else:
                # End of the processing window. process file in windows or blocks setup by log_filter.time_window
                window_counter += 1
                data_window_current.append(data_to_keep)

                # Check the window id we tacked or gybed and mark the entire band as maneuver happened in this period.
                if "Tack_Gybe_Detect" in header_index:
                    min_twa = min(data_window_current, key=lambda x: x['TWA'])
                    max_twa = max(data_window_current, key=lambda x: x['TWA'])
                    # Did the boat tack or gybe in the time window?

                    if min_twa['TWA'] * max_twa['TWA'] < 0:  # There is a + and - wind angle.
                        # print("Tack / Gybe : " + str(min_twa['TWA']) + ":" + str(max_twa['TWA']))
                        [x.update({'Tack_Gybe_Detect': 1}) for x in data_window_current]  # this may be slow ?
                        # Need to work on this still.

                # Clear out the window at the end. Maybe remove from the front when we get to the end and modulo this?
                data_writer.writerows(data_window_current)
                data_window_current.clear()
                window_counter = 1
            # ----

        except Exception as e:
            # Drop line as formatting is off simpler to drop the line than try and recover for now.
            # This could be a partial line or a line that had a value with no key pair.
            # Print any information to help with debug if code hits this area.
            print('Dropping a log line that is partial or not parsed correctly.')
            print('Log line :' + line)
            print(type(e))
            print(e)


def read_extract_keys():
    key_data = []
    with open('extract.cfg', 'r') as keys:
        for line in keys:
            if '#' not in line and not line.isspace():
                key_data = key_data + list(map(str.strip, line.strip().split(',')))

    print('Extracting these values: ...')
    print(*key_data)
    return key_data


def print_help():
    print('expedtionlogparser.py -i <input file or directory> -o <output file>')
    print('optional params: [-s 10 subsample rate each 10th sample]]')
    print('optional params: [-d drop values outside filter ranges]')
    print('optional params: [-t converts excel time to string]')
    print('optional params: [-p Polar file, if provided the logs will merge in % of target speed for TWA and TWS]')
    print('Default output file if -o is not provided is MergedData.csv in the working directory')
    sys.exit(2)


def main(argv):
    log_input = ""
    output_file = "MergedData.csv"  # Default output merged file.
    apply_filter = False
    subsample = 1  # default is keep all lines else sample each x entry
    convert_time_format = False
    polar_file = None
    polar_data = None

    # Set up the columns you need to keep in the extract.cfg file.
    # columns_to_keep = []

    try:
        opts, args = getopt.getopt(argv, "i:o:s:p:hdt", ["ifile=", "ofile=", "subsample=number",
                                                         "convert_time_to_string", "polar_file"])
        for opt, arg in opts:
            if opt == '-h':
                print_help()
            elif opt in ("-i", "--ifile"):
                log_input = arg
            elif opt in ("-o", "--ofile"):
                output_file = arg
            elif opt in ("-d", "--drop_zero_speed"):
                apply_filter = True
            elif opt in ("-s", "--subsample"):
                subsample = int(arg)
            elif opt in ("-t", "--convert_time_to_string"):
                convert_time_format = True
            elif opt in ("-p", "--polar_file"):
                polar_file = arg
            else:
                print('Unknown parameter:' + opt)
                print_help()
    except getopt.GetoptError:
        print_help()

    columns_to_keep = read_extract_keys()

    if polar_file:
        polar_data = Polars()
        polar_data.load_expedition_format(polar_file)

    log_filter = LogFilter(apply_filter, subsample, convert_time_format, 0, polar_data)
    process_files(log_input, output_file, columns_to_keep, log_filter)


if __name__ == '__main__':
    main(sys.argv[1:])

# Notes on future changes we may add or work on: KLM file format export to make easy to use Google Earth and maps
# with your tracks Drop data from file that is far from polar data. Drop data when taking, gybing and or in broach.
# Windowed time averages, sample the data over time window and return the average or median values. Preserve Local
#   preserving Max and Min values. Keep up with Expedition's file format changes etc.
