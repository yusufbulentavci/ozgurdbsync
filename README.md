
![Logo](https://github.com/yusufbulentavci/ozgurdbsync/blob/main/src/test/resources/syncicon.png)


# ozgurdbsync
Compares and generates SQL DML scripts to synchronize source tables to destination tables in different databases.
Do not make any modifications on databases; it only reports schema differences and generates DML scripts. 


# Usage
Generate an ini file to specify working environment.
Below test1 and test2 databases should have sc1 schema and tbl1 and tbl2 tables in it. If any of table definition is different, application will give an error and exits.
```
[general]
sqlEngine=postgresql
allTables=no
dataCompareDiskSizeLimitInMB=1024

[source]
url=jdbc:postgresql://localhost/test1
user=testuser
password=1

[destination]
url=jdbc:postgresql://localhost/test2
user=testuser
password=1

[table-1]
schema=sc1
table=tbl1

[table-2]
schema=sc1
table=tbl2
```


# Run
```
java -cp ./ozgurdbsync-jar-with-dependencies.jar  ozgurdbsync.Main ./my.ini
```
Output will be written to standart and error 

