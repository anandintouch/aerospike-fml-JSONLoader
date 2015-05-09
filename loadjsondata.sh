#!/bin/bash

echo "Data loading started.."
java -cp target/aerospike-fml-jsonloader-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.aerospike.customer.fml.jsonloader.AerospikeJSONParser Sample_run_json_HitData.jason localhost 3000 test fmlabset

echo "Data loading completed !"