import csv
import getopt
import os
import sys
import xlrd

from packaging import version


class LogFilter:
    # This class is used to build up all the parameters used in processing the log file.
    # Convenient way to group and pass this dara along the code path.
    drop_zero_speed: bool = False  # Default to keep all data no cleanup
    subsample: int = 1  # Default to all lines no sub sampling.
    average_samples: int = 0  # Default is no averaging
    convert_time = False

    def __init__(self, drop_zero_speed=False, subsample=1, convert_time_format=False, average_samples=0):
        self.drop_zero_speed = drop_zero_speed
        self.subsample = subsample
        self.average_samples = average_samples
        self.convert_time = convert_time_format


def convert_time(timestamp):
    # Note about time formats. Expedition uses Xls epoc time.
    # Unix Epoc time is number of seconds since Jan 1 1970 fractions of seconds are fractions in float
    # MS Excel date time starts on Jan 1 1900 and counts forward where 1 unit is a day.
    # As a ref conversion in Excel to epoc value DATE(1970,1,1))*86400
    # As a ref Conversion in Tableau calc would be Datetime(value + INT(#30 December 1899"))

    # Convert from XLS format to string
    datetime_date = xlrd.xldate_as_datetime(float(timestamp), 0)
    return datetime_date.strftime("%m/%d/%Y %H:%M:%S")


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
            data_pairs = dict(zip(headers, data))
            data_pairs["Leg_Name"] = leg_name
            data_final = {key: data_pairs.get(key, '') for key in columns_to_keep}

            # Filter if there is no instrument data.
            if data_final['BSP'] == '' or data_final['Lat'] == '' or data_final['Lon'] == '':
                continue  # boat is on and recording but no instruments are enabled.

            if log_filter.drop_zero_speed and float(data_final['BSP']) == 0 and float(data_final['SOG']) == 0:
                continue

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
            print(e)
            # print(type(e))


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
        headers["Leg_Name"] = 'Leg_Name'
        header_index = {headers[key]: key for key in columns_to_keep}

    # Build CSV header
    data_writer = csv.DictWriter(output_file, fieldnames=columns_to_keep, delimiter=',',
                                 quotechar='"', quoting=csv.QUOTE_MINIMAL)

    # Only pass the header once for the first log. (Alt check the file size here?)
    if process_header:
        data_writer.writeheader()
        output_file.flush()

    counter = 1
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
                    value = data[index + 1]

                    # if the value is one to keep print it to the new true clean csv.
                    if key in header_index:
                        data_to_keep[header_index[key]] = value

                        if log_filter.convert_time and header_index[key] == "Utc":
                            data_to_keep[header_index[key]] = convert_time(value)

                index += 1

            # Filter out lines if there is no instrument data present.
            if 'Lat' not in data_to_keep or 'Lon' not in data_to_keep:
                continue  # boat is on and recording but no instruments are enabled.

            if log_filter.drop_zero_speed and float(data_to_keep['BSP']) == 0 and float(data_to_keep['SOG']) == 0:
                continue

            if "Leg_Name" in header_index:
                data_to_keep["Leg_Name"] = leg_name

            if counter >= log_filter.subsample:
                counter = 1
                data_writer.writerow(data_to_keep)
            else:
                counter += 1

        except Exception as e:
            # Drop line as formatting is off simpler to drop the line than try and recover for now.
            # This could be a partial line or a line that had a value with no key pair.
            # Print any information to help with debug if code hits this area.
            print('Dropping a log line that is partial or not parsed correctly.')
            print('Log line :' + line)
            print(e)
            # print(type(e))


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
    print('optional params: [-s 10 subsample rate each 10th sample] [-d delete 0 speed entries]')
    print('optional params: [-t converts excel time to string]')
    print('Default output file if -o is not provided is MergedData.csv in the working directory')
    sys.exit(2)


def main(argv):
    log_input = ""
    output_file = "MergedData.csv"  # Default output merged file.
    drop_zero_speed = False
    subsample = 1  # default is keep all lines else sample each x entry
    convert_time_format = False

    # Set up the columns you need to keep in the extract.cfg file.
    # columns_to_keep = []

    try:
        opts, args = getopt.getopt(argv, "i:o:s:hdt", ["ifile=", "ofile=", "subsample=number"])
        for opt, arg in opts:
            if opt == '-h':
                print_help()
            elif opt in ("-i", "--ifile"):
                log_input = arg
            elif opt in ("-o", "--ofile"):
                output_file = arg
            elif opt in ("-d", "--drop_zero_speed"):
                drop_zero_speed = True
            elif opt in ("-s", "--subsample"):
                subsample = int(arg)
            elif opt in ("-t", "--convert_time_to_string"):
                convert_time_format = True
            else:
                print('Unknown parameter:' + opt)
                print_help()
    except getopt.GetoptError:
        print_help()

    columns_to_keep = read_extract_keys()
    log_filter = LogFilter(drop_zero_speed, subsample, convert_time_format, 0)
    process_files(log_input, output_file, columns_to_keep, log_filter)


if __name__ == '__main__':
    main(sys.argv[1:])

# Notes on future changes we may add or work on:
# KLM file format export to make easy to use Google Earth and maps with your tracks
# Drop data from file that is far from polar data.
# Drop data when taking, gybing and or in broach.
# Windowed time averages, sample the data over time window and return the average or median values.
# Keep up with Expedition's file format changes etc.
