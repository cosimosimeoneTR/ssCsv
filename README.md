# ssCsv - Simple Stupid CSV creator

##General notes
Scope of this script is produce CSVs from (Oracle) database.
It groups queries into `collections`, which are 1:1 to the CSV file you want to produce.
A collection is a JSON file containing query[es] needed to query data, header, etc.

## What's in the box:
Pulling project will download three main files:
* ```ssCsv.py``` The main script
* ```ssCsv.py.conf``` Main configuration file
* ```ssCsv.py.conn``` Main connection file

and three directories:
* ```collections/``` Here all collections are stored; see further dascription
* ```connections/``` Contains one file for each connection you might want to connect to; previous ```ssCsv.py.conn``` can be a symlink to a file in this directory
* ```log/``` Log files directory

### Global config file
The `ssCsv.py.conf` file contains global configuration.
A `ssCsv.py.conf` example:
```
# This is a comment
arraySeparator=¬
replaceCharFrom1="
replaceCharTo1=""
replaceCharFrom2=\
replaceCharTo2=\\
syncWritesEvery=9000
cxOracle_arraySize=99999
allCollectionQueryRunParallel=N
```
* The `arraySeparator` specifies the character(s) to be used as array separator (see below)
* `replaceCharFromN/replaceCharToN`is used to replace specific character(s) to other(s)
* `syncWritesEvery` tells script the write buffer size, when writing the CSV. The higher the numbre, the higher the memory use, the fastest the writes
* `cxOracle_arraySize` sets the oracle query result buffer size
* `allCollectionQueryRunParallel` tells the script to run all the queries in parallel.
	* If `N`, first query will be executed, and after that all remaining queries will be executed in parallel.
	* If `Y`, all queries will run in parallel, resulting in more memory use

### Global connection file


## What you need to provide
### Collection file
First thing, a valid connection.
Open ```ssCsv.py.conn``` and put there your connection string, as in
`scott/tiger@//localhost:1521/orcl.world`

Second, a collection file, that's where magic happens.
Let's analyze example collection file: `collection001.json`
```
{
   "outFileName" : "/tmp/collection001.csv.gz"
  ,"outFileType" : "csv"
  ,"queries":
  [
    {
     "query": "select rownum, sysdate from dual"
    ,"queryType": "simple"
    ,"queryHeader": "id, date"
   }
   ,{
     "query": "select rownum, '1' as something from dual union select rownum, '2' as something from dual"
    ,"queryType": "array"
    ,"queryHeader": "something :number[]"
   }
  ]
}
```
* `outFileName` contains CSV file path and name
* `outFileType` will be used when this script will produce XMLs, JSONs, etc as well
* `queries` Json array containing queries:
	* `query` the query itself, on a single line
	* `queryType` can be `simple`or `array`
		* `simple` means each value is considered as a "normal" value
		* `array` means that, for each ID (first value result), every value is merged with the previous one

### (Optional) Configuration file
Just as global `ssCsv.py.conf`, in the same collection file directory you can create a specific collection configuration file, called `collection001.json.conf`.
If no `.conf` provided, global one will be used.
If `collection001.json.conf` exists but does not contain all parameters, unspecified parameters will be taken from global configuration.
For exemple, you can create a  `collection001.json.conf` with just
``cxOracle_arraySize=9999``
meanign that all configuration parameters will be as in global file except `cxOracle_arraySize`which will be specified for this collection as `9999`.

### (Optional) Connection file
More of the same: if you need a specific collection to connect to a specific different database, just create `collection001.json.conn` with connection string

## Result
The previous collection file will produce this output (with debug option to 1:
```
$ ./ssCsv.py -c collections/collection001.json -d 1
2016-10-13 15:06:16,533 [          13] - Hello you!
2016-10-13 15:06:16,533 [          13] - parCollectionToRun: collections/collection001.json
2016-10-13 15:06:16,533 [          14] - parDebugLevel: 1
2016-10-13 15:06:16,536 [          17] - collectionFilesArray=collections/collection001.json
2016-10-13 15:06:16,538 [          18] - --- Now running collection collections/collection001.json
2016-10-13 15:06:16,538 [          19] - Opening file /tmp/collection001.csv.gz
2016-10-13 15:06:16,539 [          19] - - Executing query 1/2
2016-10-13 15:06:16,539 [          19] - - - This is query 0
2016-10-13 15:06:16,539 [          20] - - - DB connection: scott/tiger@//localhost:1521/orcl.world
2016-10-13 15:06:16,540 [          20] - - - cxOracle_arraySize: 99999
2016-10-13 15:06:17,575 [        1055] - Query:{select 1, sysdate from dual}
2016-10-13 15:06:17,799 [        1279] - - - DONE query 0
2016-10-13 15:06:17,801 [        1281] - - Executing query 2/2
2016-10-13 15:06:17,801 [        1281] - - - This is query 1
2016-10-13 15:06:17,801 [        1281] - - - DB connection: scott/tiger@//localhost:1521/orcl.world
2016-10-13 15:06:17,801 [        1282] - - - cxOracle_arraySize: 99999
2016-10-13 15:06:18,605 [        2085] - Query:{select 1, '1' as something from dual union select 1, '2' as something from dual}
2016-10-13 15:06:18,816 [        2296] - - - DONE query 1
2016-10-13 15:06:19,820 [        3300] - Spooling now
2016-10-13 15:06:19,820 [        3301] - Spooling every 9000 rows
2016-10-13 15:06:19,822 [        3302] - Ended elaborating collections/collection001.json; 1 rows in 3.282817 seconds
2016-10-13 15:06:19,822 [        3302] - Have a nice day!

```

### Parameter list
Here they are:

| Short parameter | Long parameter | Allowed values | Value help | Value description |
| --- | --- | --- | --- | --- |
| -c | --collectionToRun | (none) | (optional, default all collections) | Collection file to run, eg: `collections/collection001.json` |
| -d | --debugLevel|[0/1] | (optional, default 0) | Debug level (0=INFO, 1=DEBUG) |
