--Note: all code here should be executed in Beeline shell
-- if you are using hive shell, for hdfs commands, hey should start with !(instead of !sh) and end with ; 

--1. load the data
you can download the file from my repository:  
wget https://github.com/ayaditahar/hive/blob/main/data/constituents.csv

-- add data to hdfs

!sh hdfs dfs -mkdir /user/ahmed/data/constituents   
!sh hdfs dfs -put data/constituents.csv /user/ahmed/data/constituents
!sh hdfs dfs -ls /user/ahmed/data/constituents

-- Create a database:


CREATE DATABASE IF NOT EXISTS constituents_db;
USE constituents_db;
SHOW DATABASES;
+------------------+
|  database_name   |
+------------------+
| constituents_db  |
| default          |
+------------------+

--2. create a managed table

CREATE TABLE IF NOT EXISTS constituents_tb(
  symbol STRING,
  name STRING,
  sector STRING
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


--describe table

DESCRIBE FORMATTED constituents_tb;
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
|           col_name            |                     data_type                      |                      comment                       |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
| # col_name                    | data_type                                          | comment                                            |
| symbol                        | string                                             |                                                    |
| name                          | string                                             |                                                    |
| sector                        | string                                             |                                                    |
| # Detailed Table Information  | NULL                                               | NULL                                               |
| Database:                     | constituents_db                                    | NULL                                               |
| CreateTime:                   | Tue Jun 21 18:27:28 CET 2022                       | NULL                                               |
| Location:                     | hdfs://ubuntu21:9000/user/hive/warehouse/constituents_db.db/constituents_tb | NULL                                               |
| Table Type:                   | MANAGED_TABLE                                      | NULL                                               |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+



--query table
select * from constituents_tb;
+-------------------------+-----------------------+-------------------------+
| constituents_tb.symbol  | constituents_tb.name  | constituents_tb.sector  |
+-------------------------+-----------------------+-------------------------+
+-------------------------+-----------------------+-------------------------+

--you can add data to table by just running this next command

LOAD DATA INPATH '/user/ahmed/data/constituents'
INTO TABLE constituents_tb;

-now, if you query a table again , you will get results (we limit to only 5 rows to fit in screen) :

select * from constituents_tb limit 5;
+-------------------------+-----------------------+-------------------------+
| constituents_tb.symbol  | constituents_tb.name  | constituents_tb.sector  |
+-------------------------+-----------------------+-------------------------+
| Symbol                  | Name                  | Sector                  |
| MMM                     | 3M Company            | Industrials             |
| AOS                     | A.O. Smith Corp       | Industrials             |
| ABT                     | Abbott Laboratories   | Health Care             |
| ABBV                    | AbbVie Inc.           | Health Care             |
+-------------------------+-----------------------+-------------------------+


--4. delete table : now let’s delete the table: 
DROP TABLE constituents_tb;

--if you check the table location mentioned earlier in table above(describe command ), you will notice that it is gone:
!sh hdfs dfs -ls /user/hive/warehouse/constituents_db.db/

-- as well the original location of file itself:
!sh hdfs dfs -ls /user/ahmed/data/constituents
--both commands return nothing which means they are no longer exists.


--3. Create External Table

-- because data was dleted in prevous step, we have to load it again into hdfs :
!sh hdfs dfs -put data/constituents.csv /user/ahmed/data/constituents
!sh hdfs dfs -ls /user/ahmed/data/constituents
Found 1 items
-rw-r--r--   1 ahmed supergroup      19182 2022-06-21 20:26 /user/ahmed/data/constituents/constituents.csv


--to create an external table, we only have to add a word EXTERNAL after create, just like in our case:
CREATE EXTERNAL TABLE IF NOT EXISTS constituents_tb(
  symbol STRING,
  name STRING,
  sector STRING
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");


--if we look to infos about the table we just create: 
DESCRIBE FORMATTED constituents_tb;
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
|           col_name            |                     data_type                      |                      comment                       |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+
| # col_name                    | data_type                                          | comment                                            |
| symbol                        | string                                             |                                                    |
| name                          | string                                             |                                                    |
| sector                        | string                                             |                                                    |

| Database:                     | constituents_db                                    | NULL                                               |
| CreateTime:                   | Tue Jun 21 20:29:15 CET 2022                       | NULL                                               |
| Location:                     | hdfs://ubuntu21:9000/user/hive/warehouse/constituents_db.db/constituents_tb | NULL                                               |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                                               |
+-------------------------------+----------------------------------------------------+----------------------------------------------------+

--as yo can see, because we didn’t specify the location during table creation, the location by default will be the same if the table is the same. and you notice this time the table type is External as mentioned in the output.

--LOAD DATA INTO TABLE
--now let’s populate the table with some data, but before that we can check that the table is empty:
select * from constituents_tb ;
+-------------------------+-----------------------+-------------------------+
| constituents_tb.symbol  | constituents_tb.name  | constituents_tb.sector  |
+-------------------------+-----------------------+-------------------------+
+-------------------------+-----------------------+-------------------------+
No rows selected (0.27 seconds)



--you can load the previous file copied into HDFS into our table by using this syntax: 

LOAD DATA INPATH '/user/ahmed/data/constituents'
INTO TABLE constituents_tb;
No rows affected (0.429 seconds)

-- now query a table again:

select * from constituents_tb limit 6;
+-------------------------+-----------------------+-------------------------+
| constituents_tb.symbol  | constituents_tb.name  | constituents_tb.sector  |
+-------------------------+-----------------------+-------------------------+
| MMM                     | 3M Company            | Industrials             |
| AOS                     | A.O. Smith Corp       | Industrials             |
| ABT                     | Abbott Laboratories   | Health Care             |
| ABBV                    | AbbVie Inc.           | Health Care             |
| ACN                     | Accenture plc         | Information Technology  |
| ATVI                    | Activision Blizzard   | Information Technology  |
+-------------------------+-----------------------+-------------------------+
6 rows selected (0.316 seconds)


--drop table

DROP TABLE constituents_tb;
show tables;
+-----------+
| tab_name  |
+-----------+
+-----------+
No rows selected (0.101 seconds)


--check table location 
!sh hdfs dfs -ls /user/hive/warehouse/constituents_db.db/constituents_tb
Found 1 items
-rw-r--r--   1 ahmed supergroup      19182 2022-06-21 20:26 /user/hive/warehouse/constituents_db.db/constituents_tb/constituents.csv

--check the original location, it will not be there:
!sh hdfs dfs -ls /user/ahmed/data/constituents 


--Clean the data

DROP TABLE IF EXISTS constituents_tb;

DROP DATABASE IF EXISTS constituents_db;

