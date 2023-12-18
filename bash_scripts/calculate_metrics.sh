#!/bin/bash

source /home/anyakazachkova/Superjob_Parser/bash_scripts/credentials.conf

sshpass -p $ARHIMAG_PASSWORD ssh study.apxumar.ru << 'ENDSSH'

source credentials.conf
beeline -u jdbc:hive2://rc1b-dataproc-m-52f2v3qpq6q2ydjz.mdb.yandexcloud.net:10000 -n $ARHIMAG_USERNAME -p $ARHIMAG_PASSWORD << 'ENDHIVE'

DROP TABLE superjob_metrics;

CREATE EXTERNAL TABLE IF NOT EXISTS superjob_metrics (
    keyword STRING,
    min_salary DOUBLE,
    max_salary DOUBLE,
    mean_salary DOUBLE,
    median_salary DOUBLE,
    n_count DOUBLE
)
STORED AS PARQUET;

INSERT INTO 
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Python' AS keyword,
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
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'SQL' AS keyword,
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
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'linux' AS keyword,
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
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'C++' AS keyword,
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
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Go' AS keyword,
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
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Docker' AS keyword,
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
    superjob_metrics (keyword, min_salary, max_salary, mean_salary, median_salary, n_count)
    SELECT
        'Machine Learning' AS keyword,
        MIN(min_salary) min_salary, 
        MAX(min_salary) max_salary, 
        AVG(min_salary) mean_salary, 
        PERCENTILE(cast(min_salary as BIGINT), 0.5) median_salary,
        COUNT(1) n_count
    FROM 
        superjob_data 
    WHERE
        LOWER(description) LIKE '%machine learning%';


ENDHIVE

ENDSSH
