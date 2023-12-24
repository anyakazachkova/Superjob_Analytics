#!/bin/bash

source /home/anyakazachkova/Superjob_Parser/bash_scripts/credentials.conf

sshpass -p $ARHIMAG_PASSWORD ssh study.apxumar.ru << 'ENDSSH'

hdfs dfs -rm -r /user/hive/warehouse/superjob_metrics

source credentials.conf
beeline -u jdbc:hive2://rc1b-dataproc-m-52f2v3qpq6q2ydjz.mdb.yandexcloud.net:10000 -n $ARHIMAG_USERNAME -p $ARHIMAG_PASSWORD << 'ENDHIVE'

DROP TABLE superjob_metrics;

CREATE EXTERNAL TABLE superjob_metrics (
    group_name STRING,
    group_type STRING,
    min_salary DOUBLE,
    max_salary DOUBLE,
    mean_salary DOUBLE,
    median_salary DOUBLE,
    n_count DOUBLE
)
STORED AS PARQUET;

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Python' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%python%';

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'SQL' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%sql%';


INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'linux' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%linux%';

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'C++' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%c++%';

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Go' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%go%';

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Docker' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%docker%';

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Machine Learning' AS group_name,
        'keyword' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%machine learning%';

INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        experience AS group_name,
        'experience' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data
    WHERE
        experience IS NOT NULL
    GROUP BY
        experience;


INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        education AS group_name,
        'education' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data
    WHERE
        education IS NOT NULL
    GROUP BY
        education;


INSERT INTO 
    superjob_metrics (group_name, group_type, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        employment AS group_name,
        'employment' AS group_type,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data
    WHERE
        employment IS NOT NULL
    GROUP BY
        employment;

ENDHIVE

ENDSSH
