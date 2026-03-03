-- Add permissions
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu SQL Review', @content_type_id, 'menu_sqlcheck');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu SQL Analysis', @content_type_id, 'menu_sqlanalyze');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Execute SQL analysis', @content_type_id, 'sql_analyze');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Execute SQL release workflow (resource-group scope)', @content_type_id, 'sql_execute_for_resource_group');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Purge BINLOG logs', @content_type_id, 'binlog_del');


-- Add instance foreign keys for SQL workflows, query privileges, RDS, and masking configuration
SET FOREIGN_KEY_CHECKS = 0;
ALTER TABLE sql_workflow
  ADD COLUMN instance_id int(11) NOT NULL AFTER group_name,
  ADD INDEX idx_instance_id (instance_id) USING BTREE,
  ADD CONSTRAINT fk_workflow_instance FOREIGN KEY fk_workflow_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE query_privileges
  ADD COLUMN instance_id int(11) NOT NULL AFTER user_display,
  ADD INDEX idx_instance_id (instance_id) USING BTREE,
  ADD CONSTRAINT fk_query_priv_instance FOREIGN KEY fk_query_priv_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE query_privileges_apply
  ADD COLUMN instance_id int(11) NOT NULL AFTER user_display,
  ADD INDEX idx_instance_id (instance_id) USING BTREE,
  ADD CONSTRAINT fk_query_priv_apply_instance FOREIGN KEY fk_query_priv_apply_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE data_masking_columns
  ADD COLUMN instance_id int(11) NOT NULL AFTER active,
  ADD INDEX idx_instance_id (instance_id) USING BTREE,
  ADD CONSTRAINT fk_data_mask_instance FOREIGN KEY fk_data_mask_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE aliyun_rds_config
  ADD COLUMN instance_id int(11) NOT NULL FIRST,
  ADD INDEX idx_instance_id (instance_id) USING BTREE,
  ADD CONSTRAINT fk_rds_instance FOREIGN KEY fk_rds_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
SET FOREIGN_KEY_CHECKS = 1;

-- Update instance_id values; if instance_name was changed previously, some data may not match and must be corrected manually
UPDATE sql_workflow sw JOIN sql_instance si on sw.instance_name = si.instance_name SET sw.instance_id=si.id;
UPDATE query_privileges qp JOIN sql_instance si on qp.instance_name = si.instance_name SET qp.instance_id=si.id;
UPDATE query_privileges_apply qpa JOIN sql_instance si on qpa.instance_name = si.instance_name SET qpa.instance_id=si.id;
UPDATE data_masking_columns dmc JOIN sql_instance si on dmc.instance_name = si.instance_name SET dmc.instance_id=si.id;
UPDATE aliyun_rds_config rds JOIN sql_instance si on rds.instance_name = si.instance_name SET rds.instance_id=si.id;

-- Drop instance_name columns
ALTER TABLE query_privileges DROP COLUMN instance_name;
ALTER TABLE query_privileges_apply DROP COLUMN instance_name;
ALTER TABLE sql_workflow DROP COLUMN instance_name;
ALTER TABLE data_masking_columns DROP COLUMN instance_name;
ALTER TABLE aliyun_rds_config DROP COLUMN instance_name;


-- Rename sql_syntax to syntax_type and drop audit_remark/reviewok_time
ALTER TABLE sql_workflow 
  CHANGE sql_syntax  syntax_type tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Workflow type: 1=DDL, 2=DML',
  DROP audit_remark,
  DROP reviewok_time;

-- Change db_name/table_name length to 64
ALTER TABLE sql_workflow MODIFY  db_name varchar(64) NOT NULL DEFAULT '' COMMENT 'Database' AFTER instance_id;
ALTER TABLE query_privileges MODIFY  db_name varchar(64) NOT NULL DEFAULT '' COMMENT 'Database' AFTER instance_id;
ALTER TABLE query_privileges MODIFY  table_name varchar(64) NOT NULL DEFAULT '' COMMENT 'Table' AFTER instance_id;
ALTER TABLE query_log MODIFY db_name varchar(64) NOT NULL DEFAULT '' COMMENT 'Database' AFTER instance_name;



-- Split large SQL workflow fields into a dedicated content table
CREATE TABLE sql_workflow_content(
  id             int(11)  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  workflow_id    int(11)  NOT NULL COMMENT 'SQL workflow ID',
  sql_content    longtext NOT NULL COMMENT 'Submitted SQL text',
  review_content longtext NOT NULL COMMENT 'Auto-review content in JSON format',
  execute_result longtext NOT NULL COMMENT 'Execution result in JSON format',
  UNIQUE KEY uniq_workflow_id (workflow_id) USING BTREE,
  CONSTRAINT fk_cont_workflow FOREIGN KEY fk_cont_workflow (workflow_id) REFERENCES sql_workflow (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);
-- Data migration
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO sql_workflow_content (workflow_id, review_content, sql_content, execute_result)
SELECT id, review_content, sql_content, execute_result
FROM sql_workflow;
SET FOREIGN_KEY_CHECKS = 1;
-- Drop columns
ALTER TABLE sql_workflow DROP sql_content, DROP review_content, DROP execute_result;
