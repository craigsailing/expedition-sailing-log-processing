# expedition-sailing-log-processing
Process 'Expedition Marine Navigation' logs: merge, clean and extract logs to a csv format usable by other data analytics. 

This was created to help sailors cleanup and process their logs from Expedition. By default, 'Expedition' has a log per day and on longer trips need to be merged for analysis. 
Sometimes there can be corrupted entries or partial entries in the log these are stripped when processing the data.

You can also export your data via Expedition to a true CSV files for data analysis, this program just aids you in doing so in bulk format.

**Data to be Extracted to CVS file**
The columns you wish to extract are in the **extract.cfg file**. Modify this file as needed to extract the data you want.

**Run time options:**

python expedtionlogparser.py -i input file or directory -o output file 

optional params: [-s 10 subsample rate each 10th sample]

optional params: [-d delete 0 speed entries]

optional params: [-t converts the default MS Excel time to a string format]

The default output file if -o is not provided is MergedData.csv in the working directory