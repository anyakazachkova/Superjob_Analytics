#!/bin/bash

beeline -u jdbc:hive2://rc1b-dataproc-m-52f2v3qpq6q2ydjz.mdb.yandexcloud.net:10000 -n anyakazachkova -p UMRD91 << 'ENDHIVE'

CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS ak_temp (
    name STRING,
    url STRING,
    salary STRING,
    education STRING,
    address STRING,
    experience STRING,
    employment STRING,
    description STRING
)
STORED AS PARQUET;

hdfs dfs -mkdir /user/anyakazachkova/Superjob_Parser/
hdfs dfs -put Superjob_Parser/results/parsed_data /user/anyakazachkova/Superjob_Parser/

LOAD DATA INPATH '/user/anyakazachkova/Superjob_Parser/'
    INTO TABLE ak_temp;

SELECT
    *
FROM
    ak_temp
LIMIT
    10;

ENDHIVE