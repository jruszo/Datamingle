-- Add permissions
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Parameter Configuration', @content_type_id, 'menu_param');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('View instance parameter list', @content_type_id, 'param_view');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Edit instance parameters', @content_type_id, 'param_edit');

-- Update boolean values
-- sql_workflow.is_backup
UPDATE sql_workflow SET is_backup=1 WHERE is_backup='Yes';
UPDATE sql_workflow SET is_backup=0 WHERE is_backup='No';
ALTER TABLE sql_workflow
  MODIFY is_backup TINYINT NOT NULL DEFAULT 1 COMMENT 'Whether backup is enabled';

-- data_masking_columns.active
ALTER TABLE data_masking_columns
  MODIFY active TINYINT NOT NULL DEFAULT 0 COMMENT 'Active status';

-- query_log.masking
UPDATE query_log SET priv_check=0 WHERE priv_check=2;
UPDATE query_log SET hit_rule=0 WHERE hit_rule=2;
UPDATE query_log SET masking=0 WHERE masking=2;
ALTER TABLE query_log
  MODIFY priv_check TINYINT NOT NULL DEFAULT 0 COMMENT 'Whether query privilege check is valid',
  MODIFY hit_rule TINYINT NOT NULL DEFAULT 0 COMMENT 'Whether query hit masking rules',
  MODIFY masking TINYINT NOT NULL DEFAULT 0 COMMENT 'Whether query result is properly masked';

-- aliyun_access_key.is_enable
UPDATE aliyun_access_key SET is_enable=0 WHERE is_enable=2;
ALTER TABLE aliyun_access_key
  MODIFY is_enable TINYINT NOT NULL DEFAULT 1 COMMENT 'Whether enabled';

-- aliyun_rds_config.is_enable
UPDATE aliyun_rds_config SET is_enable=0 WHERE is_enable=2;
ALTER TABLE aliyun_rds_config
  MODIFY is_enable TINYINT NOT NULL DEFAULT 1 COMMENT 'Whether enabled';

-- Add default values for username and password
ALTER TABLE sql_instance
  MODIFY `user` VARCHAR(200) NOT NULL DEFAULT '' COMMENT 'Username',
  MODIFY `password` VARCHAR(200)  NOT NULL DEFAULT '' COMMENT 'Password';

-- Add index to query privileges table
ALTER TABLE query_privileges
  ADD INDEX  idx_user_name_instance_id_db_name_valid_date(user_name,instance_id,db_name,valid_date);

-- Instance parameter configuration
CREATE TABLE param_template(
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
  db_type VARCHAR(10) NOT NULL COMMENT 'Database type: mysql, mssql, redis, pgsql',
  variable_name VARCHAR(64) NOT NULL COMMENT 'Parameter name',
  default_value VARCHAR(1024) NOT NULL COMMENT 'Default parameter value',
  editable TINYINT NOT NULL COMMENT 'Whether editable',
  valid_values VARCHAR(1024) NOT NULL COMMENT 'Valid parameter values',
  description VARCHAR(1024) NOT NULL COMMENT 'Parameter description',
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
  sys_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Create time',
  UNIQUE uniq_db_type_variable_name(db_type, variable_name)
) COMMENT 'Instance parameter configuration table';



CREATE TABLE param_history(
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
  instance_id INT NOT NULL COMMENT 'Instance ID',
  variable_name VARCHAR(64) NOT NULL COMMENT 'Parameter name',
  old_var VARCHAR(1024) NOT NULL COMMENT 'Parameter value before change',
  new_var VARCHAR(1024) NOT NULL COMMENT 'Parameter value after change',
  set_sql VARCHAR(1024) NOT NULL COMMENT 'Executed SQL for online config change',
  user_name VARCHAR(30) NOT NULL COMMENT 'Operator',
  user_display VARCHAR(50) NOT NULL COMMENT 'Operator display name',
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Update time',
  INDEX idx_variable_name(variable_name),
  CONSTRAINT fk_param_instance FOREIGN KEY fk_param_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT
) COMMENT 'Instance parameter change history';
