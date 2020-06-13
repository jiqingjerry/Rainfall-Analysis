###CS236 Project

#Jerry Zhu

#System Requirements:
	Java openjdk version "1.8.0_252"
	Sbt 1.3.12
	Scala 2.11.12
	Spark 2.4.5

##Description
The locations dataset contains station information from weather stations across the world. The recordings dataset provides individual recordings for the stations over a 4-year period. The goal of the project is to find out which states in the US have the most stable rainfall. That is, the result should provide the US states ordered (in ascending order) by D, where D is the difference between the two months with the highest and lowest rainfall.


#Compiling the code
	- If you need to compile the code, do 'sbt package', make sure 'build.sbt' and your '.scala' file are in the same folder

#Running the program
	- Open a terminal in the direction with the jar file and shell script
	- Execute run.sh with arguments:
		- './run.sh [Locations file folder path] [Recordings file folder path] [output folder path]''
		- **make sure the output doesn't exist before running**

#Quick overview of the steps within the job
	- Read in location and recording fils into Resilient Distributed Datasets(RDD)
	- Filter locations RDD to have only US States
	- Filter recordings RDD that has 99.99 for precipitation, because they are excluded from calculations
	- Join the two RDD with USAF/STN---, STATE as the key, as they are matching values
	- Calculate precipitation
	- Group stations by state
	- Find the average precipitation for each month per state
	- Find the month with the highest and lowest averages for each state
	- Order states by highest - lowest difference.

#The total runtime of the project is around 75 seconds

#Details on the more important steps
	- The 'map()' function for RDDs are used the most for this job.
	- Parsing the data
		- Use 'map()' function to select only the columns that are needed for the job -> UCAF, CTRY, STATE, STN---, YEARMODA, PRCP
		- Join them together with 'join()', having UCAF/STN--- and STATE as key, then STATE, YEARMODA and PRCP as value. Thus creating a piared RDD
		- paired RDD can perform operations using a Key for grouping, which is needed to group by States and month
	- Calculate monthly average
		- Remove day from the dates as it is not needed
		- use 'reduceByKey()' to get the monthly average, key is '(State, Month)'
	- Get Max Min and difference
		- use 'reduceByKey()' to get max RDD and min RDD, key is '(State)'
		- 'join()' max RDD and min RDD with max-min to get the difference, 'SortBY()' to sort ascending.

#Dataset Information
The LOCATIONS dataset is a single .csv file (WeatherStationLocations.csv), containing the metadata for every station across the world. To identify that a station is in the US, you need to look for stations where the “CTRY” field is “US” and the “ST” field is non-empty. Keep in mind that the first row of this file is Header. Here are the fields for this dataset:
	- USAF = Air Force station ID. May contain a letter in the first position.
	- WBAN = NCDC WBAN number
	- CTRY = FIPS country ID
	- ST = State for US stations
	- LAT = Latitude in thousandths of decimal degrees
	- LON = Longitude in thousandths of decimal degrees
	- ELEV = Elevation in meters
	- BEGIN = Beginning Period Of Record (YYYYMMDD). 
	- END = Ending Period Of Record (YYYYMMDD). 
	- Sample Row:
		-"724920","23237","STOCKTON METROPOLITAN AIRPORT","US","CA","+37.889","-121.226","+0007.9","20050101","20140403”

The RECORDINGS dataset is contained in four files, one for each year (2006-2009). The “STN---“ value will match with the “USAF” field in the locations dataset. These files are concatenated from many small files, so keep in mind that there will be Header lines through the files. Here are the fields for this dataset:
	- STN---  = The station ID (USAF)
	- WBAN   = NCDC WBAN number
	- YEARMODA   = The datestamp
	- TEMP = Ignore for this project
	- DEWP = Ignore for this project
	- SLP = Ignore for this project
	- STP = Ignore for this project
	- VISIB = Ignore for this project (Visibility)
	- WDSP = Ignore for this project
	- MXSPD = Ignore for this project
	- GUST = Ignore for this project    
	- MAX = Ignore for this project (Max Temperature for the day)
	- MIN = Ignore for this project (Min Temperature for the day)
	- PRCP = Precipitation
	- NDP = Ignore for this project   
	- FRSHTT = Ignore for this project
	- Sample Row: 
		- 997781 99999  20061121    42.4 13  9999.9  0  9999.9  0  9999.9  0  999.9  0   17.5 13   22.0  999.9    46.2*   39.0*  0.00I 999.9  000000
