#!/bin/bash
DIR="$( cd "$( dirname "$0"  )" && pwd  )"
cd ${DIR}

# Configure Archery database connection
archery_db_host="127.0.0.1"
archery_db_port=3306
archery_db_user="root"
archery_db_password="123456"
archery_db_database="archery"

# Slow log path of the analyzed instance.
# Periodically clean log files to keep analysis efficient.
slowquery_file="/home/mysql/log_slow.log"

# Path to pt-query-digest executable
pt_query_digest="/usr/bin/pt-query-digest"

# Connection identifier of the analyzed instance
hostname="mysql_host:mysql_port" # Must match the value configured in Archery for filtering; mismatches will prevent data display

# Get last analysis time.
# For initial full analysis, remove last_analysis_time_$hostname.
if [[ -s last_analysis_time_${hostname} ]]; then
    last_analysis_time=`cat last_analysis_time_${hostname}`
else
    last_analysis_time='1000-01-01 00:00:00'
fi

# Collect logs
# Add --no-version-check for RDS
${pt_query_digest} \
--user=${archery_db_user} --password=${archery_db_password} --host=${archery_db_host} --port=${archery_db_port} \
--review h=${archery_db_host},D=${archery_db_database},t=mysql_slow_query_review  \
--history h=${archery_db_host},D=${archery_db_database},t=mysql_slow_query_review_history  \
--no-report --limit=100% --charset=utf8 \
--since "$last_analysis_time" \
--filter="\$event->{Bytes} = length(\$event->{arg}) and \$event->{hostname}=\"$hostname\"  and \$event->{client}=\$event->{ip} " \
${slowquery_file} > /tmp/analysis_slow_query.log

if [[ $? -ne 0 ]]; then
echo "failed"
else
echo `date +"%Y-%m-%d %H:%M:%S"`>last_analysis_time_${hostname}
fi
