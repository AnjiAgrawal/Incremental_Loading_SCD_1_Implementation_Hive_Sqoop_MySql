#!/bin/bash
. /user/saif/cohort_f11/env/sqp.prm
start-all.sh
mysql -uroot -pWelcome@123 -e "
create database if not exists Project1;
use Project1;
create table if not exists tbl_day(
custid INTEGER PRIMARY KEY,
username VARCHAR(100),
quote_count INTEGER,
ip VARCHAR(30),
entry_time VARCHAR(100),
prp_1 INTEGER,
prp_2 INTEGER,
prp_3 INTEGER,
ms INTEGER,
http_type VARCHAR(10), 
purchase_category VARCHAR(100),
total_count INTEGER,
purchase_sub_category VARCHAR(100),
http_info VARCHAR(500),
status_code VARCHAR(10),
table_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
entry_year INTEGER,
entry_month INTEGER
);"

mysql -uroot -pWelcome@123 -e "
use Project1;
create table if not exists data_hive_exp (
custid INTEGER PRIMARY KEY,
username VARCHAR(100),
quote_count INTEGER,
ip VARCHAR(30),
entry_time VARCHAR(100),
prp_1 INTEGER,
prp_2 INTEGER,
prp_3 INTEGER,
ms INTEGER,
http_type VARCHAR(10), 
purchase_category VARCHAR(100),
total_count INTEGER,
purchase_sub_category VARCHAR(100),
http_info VARCHAR(500),
status_code VARCHAR(10),
table_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
entry_year INTEGER,
entry_month INTEGER
);"

sqoop job --create inc_project_id -- import \
--connect jdbc:mysql://localhost:3306/project1?useSSL=False \
--username root --password-file file:///home/saif/cohort_f11/datasets/sqoop.pwd \
-m 1 \
--delete-target-dir --target-dir /user/saif/HFS/Input/tbl_day \
--query "select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code,tmstmp,year,month from tbl_day where \$CONDITIONS"

nohup hive --service metastore &

hive -e "create database if not exists Project1;"

hive -e "use Project1;
create table if not exists mgd_tbl_day(
custid int,
username string,
quote_count int,
ip string,
entry_time string,
prp_1 int,
prp_2 int,
prp_3 int,
ms int,
http_type string, 
purchase_category string,
total_count int,
purchase_sub_category string,
http_info string,
status_code int,
table_load_date timestamp,
entry_year int,
entry_month int
)
row format delimited fields terminated by ',';"

hive -e "use Project1;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;

create table if not exists scd_tbl_day(
custid int,
username string,
quote_count int,
ip string,
entry_time string,
prp_1 int,
prp_2 int,
prp_3 int,
ms int,
http_type string, 
purchase_category string,
total_count int,
purchase_sub_category string,
http_info string,
status_code int,
table_load_date timestamp,
entry_year int,
entry_month int
)
row format delimited fields terminated by ','
stored as orc
TBLPROPERTIES('transactional'='true');"


hive -e "use Project1;
create external table if not exists ext_tbl_day (
custid int,
username string,
quote_count int,
ip string,
entry_time string,
prp_1 int,
prp_2 int,
prp_3 int,
ms int,
http_type string, 
purchase_category string,
total_count int,
purchase_sub_category string,
http_info string,
status_code int,
table_load_date timestamp
)
partitioned by (entry_year int,entry_month in)
row format delimited fields terminated by ',';"
