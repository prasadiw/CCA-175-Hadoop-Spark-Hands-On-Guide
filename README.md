# CCA 175 - Hadoop & Sparks Hands-On Guide

###Technologies: Sqoop, Flume, HDFS, Spark (Python & Scala), Hive, Impala, Avro schema, SQL, MySQL in Windows and Linux environments.
###Key Features:
•	Data Ingest: Transferring data between an external system and the CDH5 cluster, merge/update data sets, write sqoop-jobs using Sqoop in order to satisfy various use cases. Perform RT/NRT data streaming into HDFS using Flume.
•	Transform, Stage and Store: Data loading and performing appropriate logical operations (joining, aggregating, filtering, sorting, ranking) to satisfy required tasks using Spark (Python & Scala). 
•	Data Analysis: Perform data analysis by applying proper DDL, DML operations to a given schema in the hive meta-store using Hive & Impala. Perform basic Avro-tools operations on a data set.

Below mentioned features are covered in this hands-on projects/exercises.

###Data Ingest:

    •	Import data into HDFS from MySQL database using Sqoop.
  
      o	Import tables, subset of data.
    
      o	Incremental import.
    
      o	Change delimiters and file formats during import.
    
      o	Controlling parallelism.
    
      o	Importing data in to Hive.
    
    •	Export data from HDFS into MySQL database using Sqoop.
  
      o	Export data in to existing tables in MySQL.
    
      o	Update/Insert data into existing tables in MySQL.
    
      o	Handling delimiters and null values during export. 
    
    •	Merge two datasets in HDFS and store them in HDFS.
  
    •	Create Sqoop jobs to create and save Sqoop import and Sqoop export commands.
  
    •	Ingest real-time and near real time (NRT) streaming data into HDFS using Flume.
  
    •	Use Hadoop File System (FS) commands to load data into and out of HDFS.



###Transform, Stage, Store: Writing and running Spark Applications using Python and Scala.

    •	Load data from HDFS and store results back to HDFS using Spark.
  
    •	Join different datasets together using Spark.
  
    •	Calculate aggregate statistics (e.g., average or sum) using Spark.
  
    •	Filter data into a smaller dataset using Spark.
  
    •	Produce ranked or sorted data from a given dataset using Spark.
  
  

###Data Analysis: Use DDL aspects of Hive Impala, basic DML aspects of hive Impala as well as concepts around Avro-tools.

    •	Hive DDL: Read and/or create a table in the Hive metastore in a given schema.
  
    •	Hive DML: Load and Insert data into Hive tables.
  
    •	Extract an Avro schema from a set of data files using Avro-tools.
  
    •	Create a table in the Hive metastore using the Avro file format and using external schema file.
  
    •	Use hive partitioning to improve query performance of tables in the Hive metastore.
  
    •	Evolve an Avro schema by changing JSON files.


