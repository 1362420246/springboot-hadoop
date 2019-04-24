#!/bin/bash
sqoop export \
--connect jdbc:mysql://192.168.11.233:3306/db03  \
--username root  \
--password 123456 \
--table tab_user \
--export-dir /user/hive/warehouse/tab_user/part-m-00000 \
--input-fields-terminated-by '\0001'  \
--input-lines-terminated-by   '\n'  

