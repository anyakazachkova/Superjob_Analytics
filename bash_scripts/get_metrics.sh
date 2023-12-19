#!/bin/bash

source /home/anyakazachkova/Superjob_Parser/bash_scripts/credentials.conf

sshpass -p $ARHIMAG_PASSWORD ssh study.apxumar.ru << 'ENDSSH'

source credentials.conf
beeline -u jdbc:hive2://rc1b-dataproc-m-52f2v3qpq6q2ydjz.mdb.yandexcloud.net:10000 -e "SELECT DISTINCT * FROM superjob_metrics WHERE keyword IS NOT NULL;" > superjob_metrics.csv
ENDSSH

sshpass -p $ARHIMAG_PASSWORD scp anyakazachkova@study.apxumar.ru:/home/anyakazachkova/superjob_metrics.csv  /home/anyakazachkova/Superjob_Parser/results/