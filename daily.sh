#!/bin/bash\
dataset=$1
. /user/saif/cohort_f11/env/sqp.prm

start-all.sh

mysql -uroot -pWelcome@123 -e "
set GLOBAL local_infile=1;
quit;"
mysql --local-infile=1 -uroot -pWelcome@123 -e "
use Project1;
truncate tbl_day;
LOAD DATA LOCAL INFILE '/user/saif/cohort_f11/archive/${dataset}' 
INTO TABLE tbl_day 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
(custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code,@entry_year,@entry_month,@entry_day)
SET entry_year = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%Y'),entry_month = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%m');"

sqoop job --exec inc_project_id

nohup hive --service metastore &

hive -e "
use Project1;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
set hive.auto.convert.join=false;
truncate table mgd_tbl_day;
load data inpath '/user/saif/HFS/Output/mysql/*' into table mgd_tbl_day;
insert into table data_load_ext partition (entry_year,entry_month,entry_day) select custid,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category, http_info,status_code,table_load_date,entry_year,entry_month,entry_day from mgd_tbl_day;
MERGE INTO scd_tbl_day
USING mgd_tbl_day as y
ON scd_tbl_day.custid = y.custid
WHEN MATCHED AND (scd_tbl_day.status_code != y.status_code)
THEN
    UPDATE SET status_code = y.status_code
WHEN NOT MATCHED THEN
    INSERT VALUES(y.custid, y.username, y.quote_count, y.ip, y.entry_time, y.prp_1, y.prp_2, y.prp_3, y.ms, y.http_type, y.purchase_category, y.total_count, y.purchase_sub_category, y.http_info, y.status_code, y.table_load_date, y.entry_year, y.entry_month, y.entry_day);
INSERT OVERWRITE LOCAL DIRECTORY '/home/saif/cohort_f11/datasets/hive_export/'ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM scd_tbl_day;"

mysql --local-infile=1 -uroot -pWelcome@123 -e "
use Project1;
truncate data_hive_exp;
LOAD DATA LOCAL INFILE '/home/saif/cohort_f11/datasets/hive_export/*' 
INTO TABLE data_hive_exp  
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'"

mysql --local-infile=1 -uroot -pWelcome@123 -e "
use Project1; 
with e as (select count(distinct custid) as cnt from data_import_backup),
d as (select count(distinct custid) as cnt from data_hive_exp)
select abs(e.cnt-d.cnt) as ReconValue
from e,d;
