#!/bin/bash

source /home/anyakazachkova/Superjob_Parser/bash_scripts/credentials.conf

sshpass -p $ARHIMAG_PASSWORD scp -r Superjob_Parser/results/parsed_data $CLUSTER_DATA_PATH
sshpass -p $ARHIMAG_PASSWORD ssh study.apxumar.ru << 'ENDSSH'

source credentials.conf
hdfs dfs -put $CLUSTER_DATA_PATH/parsed_data /user/anyakazachkova/Superjob_Parser/

beeline -u jdbc:hive2://rc1b-dataproc-m-52f2v3qpq6q2ydjz.mdb.yandexcloud.net:10000 -n $ARHIMAG_USERNAME -p $ARHIMAG_PASSWORD << 'ENDHIVE'

CREATE EXTERNAL TABLE IF NOT EXISTS ak_temp (
    name STRING,
    url STRING,
    min_salary DOUBLE,
    max_salary DOUBLE,
    education STRING,
    address STRING,
    experience STRING,
    employment STRING,
    description STRING
)
STORED AS PARQUET;

LOAD DATA INPATH '/user/anyakazachkova/Superjob_Parser/parsed_data/'
    INTO TABLE ak_temp;

INSERT INTO 
    superjob_data (
        name, 
        url, 
        min_salary, 
        max_salary, 
        education, 
        address, 
        experience, 
        employment, 
        description
    )
SELECT
    name, 
    url, 
    min_salary, 
    max_salary, 
    education, 
    address, 
    experience, 
    employment, 
    description
FROM ak_temp
WHERE NOT EXISTS (
    SELECT 1
    FROM superjob_data
    WHERE superjob_data.url = ak_temp.url
);

ENDHIVE

ENDSSH