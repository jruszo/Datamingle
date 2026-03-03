-- 2024-9
ALTER TABLE sql_instance ADD verify_ssl tinyint(1) DEFAULT 1  COMMENT 'Whether to verify server SSL certificate. 1: verify. 0: do not verify';
ALTER TABLE sql_instance ADD show_db_name_regex varchar(1024) DEFAULT ''  COMMENT 'Regex for shown database list';
ALTER TABLE sql_instance ADD denied_db_name_regex varchar(1024) DEFAULT ''  COMMENT 'Regex for hidden database list';
 
