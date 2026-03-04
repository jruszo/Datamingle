-- Upstream reference: Archery PR #2108
alter table instance_account
    add db_name varchar(128) default '' not null comment 'Database name (mongodb)' after host;

-- Adjust unique index on instance_account table
set @drop_sql=(select concat('alter table instance_account drop index ', constraint_name) from information_schema.table_constraints where table_schema=database() and table_name='instance_account' and constraint_type='UNIQUE');
prepare stmt from @drop_sql;
execute stmt;
drop prepare stmt;
alter table instance_account add unique index uidx_instanceid_user_host_dbname(`instance_id`, `user`, `host`, `db_name`);
--- Add SSL support
ALTER TABLE sql_instance ADD is_ssl tinyint(1) DEFAULT 0  COMMENT 'Whether SSL is enabled';
