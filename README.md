
**Aerospike-Json-Loader**
=====================================
This repository contains "aerospike-jsonloader" tool which can be used to process huge file size in a stream and load database at a faster rate.Sample .json file is included in the project to load data and "AerospikeJSONParser" file can be modified to load specific element/object of the Json as per need base.


----------
**Prerequisite:**

 1. Aerospike database Server - If you don't have it installed, [click](http://www.aerospike.com/docs/operations/install/) here to install it.

**To run this tool:**
 1. The source code can be imported into your IDE or local folder using git clone command and build the maven module using Maven command as below
 
    mvn clean install
 2. Sample command line argument to run the loader is mentioned below.

> ./loadjsondata.sh

   This script points to the executable jar (aerospike-fml-jsonloader-1.0.0-SNAPSHOT-jar-with-dependencies.jar) available inside the target folder, sample json file "Sample_run_json_HitData.jason" and also hostname,port,namespace and set/table name where data need to be loaded .


