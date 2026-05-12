# Adding masking fields manually is cumbersome.
# You can use the script below and run it on a schedule.

# Masking rules
# (1, 'Phone Number'), (2, 'ID Number'), (3, 'Bank Card'), (4, 'Email'), (5, 'Amount'), (6, 'Other')
masking_rule_phone='phone|mobile'
masking_rule_idno='id_number|idcard'
masking_rule_bankcardno='bank_no'
masking_rule_mail='mail|email'
masking_rule_amount='pay_money|amount'
masking_rule_others='pwd|password|user_pass'
masking_rules="$masking_rule_phone|$masking_rule_idno|$masking_rule_bankcardno|$masking_rule_mail|$masking_rule_amount|$masking_rule_others";

DIR="$( cd "$( dirname "$0"  )" && pwd  )"
cd $DIR
archery_host=127.0.0.1
archery_port=3306 
archery_user=
archery_db=archery
archery_pw=

# Get all Archery slave instance info
mysql -h$archery_host -P$archery_port -u$archery_user -p$archery_pw $archery_db -N -e "select 
id,instance_name,host,port 
from sql_instance  where type='slave';">instances.list

# Truncate table
mysql -h$archery_host -P$archery_port -u$archery_user -p$archery_pw $archery_db -N -e "truncate table data_masking_columns;"

# Temporary account/password (instance credentials are encrypted, so use fixed values here)
# This method only works for a single instance or multiple instances sharing the same credentials
user=
pw=

# Get masking field information
cat instances.list|while read instance_name host port 
do 
mysql -h$host -P$port -u$user -p$pw -N -e "
SELECT CASE
         WHEN COLUMN_NAME REGEXP '$masking_rule_phone'
           THEN 1
         WHEN COLUMN_NAME REGEXP '$masking_rule_idno'
           THEN 2
         WHEN COLUMN_NAME REGEXP '$masking_rule_bankcardno'
           THEN 3
         WHEN COLUMN_NAME REGEXP '$masking_rule_mail'
           THEN 4
         WHEN COLUMN_NAME REGEXP '$masking_rule_amount'
           THEN 5
         WHEN COLUMN_NAME REGEXP '$masking_rule_others'
           THEN 6
         END AS       rule_type,
       1     AS       active,
       '$instance_id' instance_id,
       TABLE_SCHEMA   table_schema,
       TABLE_NAME     table_name,
       COLUMN_NAME    column_name,
       COLUMN_COMMENT column_comment
FROM information_schema.COLUMNS
WHERE COLUMN_NAME REGEXP '$masking_rules'
AND TABLE_SCHEMA != 'performance_schema'
AND TABLE_SCHEMA != 'information_schema';">$instance_name.txt

# Update table data
mysql -h$archery_host -P$archery_port -u$archery_user -p$archery_pw $archery_db -N -e "load data local infile '$instance_name.txt' replace into table data_masking_columns fields terminated by '\t' ( rule_type,active,instance_id,table_schema,table_name,column_name,column_comment);"
done
