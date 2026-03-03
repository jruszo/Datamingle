-- This SQL only improves comments and does not change definitions; use as needed.
ALTER TABLE
  aliyun_access_key COMMENT 'Alibaba Cloud credential info',
    MODIFY  `ak` VARCHAR (50) NOT NULL COMMENT 'AccessKey',
    MODIFY  `secret` VARCHAR (100) NOT NULL COMMENT 'Secret',
    MODIFY  `is_enable` TINYINT (4) NOT NULL COMMENT 'Whether enabled',
    MODIFY  `remark` VARCHAR (50) NOT NULL COMMENT 'Remark';


ALTER TABLE
  aliyun_rds_config COMMENT 'Alibaba Cloud RDS configuration',
    MODIFY  `instance_id` INT (11) NOT NULL COMMENT 'Instance ID',
    MODIFY  `rds_dbinstanceid` VARCHAR (100) NOT NULL COMMENT 'Mapped Alibaba Cloud RDS instance ID',
    MODIFY  `is_enable` TINYINT (4) NOT NULL COMMENT 'Whether enabled';


ALTER TABLE
  auth_group COMMENT 'Permission group',
    MODIFY  `name` VARCHAR (80) NOT NULL COMMENT 'Group';


ALTER TABLE
  auth_group_permissions COMMENT 'Selected permissions for permission group',
    MODIFY  `group_id` INT (11) NOT NULL COMMENT 'Group ID',
    MODIFY  `permission_id` INT (11) NOT NULL COMMENT 'Selected permission';


ALTER TABLE
  auth_permission COMMENT 'Available permissions for permission groups - custom business permissions',
    MODIFY  `name` VARCHAR (255) NOT NULL COMMENT 'Available permission name for permission group',
    MODIFY  `content_type_id` INT (11) NOT NULL COMMENT 'Permission category ID',
    MODIFY  `codename` VARCHAR (100) NOT NULL COMMENT 'ORM code name';


ALTER TABLE
  data_masking_columns COMMENT 'Data masking column configuration',
    MODIFY  `column_id` INT (11) NOT NULL AUTO_INCREMENT COMMENT 'Column ID',
    MODIFY  `rule_type` INT (11) NOT NULL COMMENT 'Rule type',
    MODIFY  `active` TINYINT (4) NOT NULL COMMENT 'Active status',
    MODIFY  `instance_id` INT (11) NOT NULL COMMENT 'Instance ID',
    MODIFY  `table_schema` VARCHAR (64) NOT NULL COMMENT 'Database name containing the column',
    MODIFY  `table_name` VARCHAR (64) NOT NULL COMMENT 'Table name containing the column',
    MODIFY  `column_name` VARCHAR (64) NOT NULL COMMENT 'Column name',
    MODIFY  `column_comment` VARCHAR (1024) NOT NULL COMMENT 'Column description',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  data_masking_rules COMMENT 'Data masking rule configuration',
    MODIFY  `rule_type` INT (11) NOT NULL COMMENT 'Rule type',
    MODIFY  `rule_regex` VARCHAR (255) NOT NULL COMMENT 'Regex used for masking; expression must use groups; hidden groups are replaced with ****',
    MODIFY  `hide_group` INT (11) NOT NULL COMMENT 'Group to hide',
    MODIFY  `rule_desc` VARCHAR (100) NOT NULL COMMENT 'Rule description',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  mysql_slow_query_review COMMENT 'Slow query statistics',
    MODIFY  `checksum` CHAR (32) NOT NULL COMMENT 'Checksum',
    MODIFY  `fingerprint` LONGTEXT NOT NULL COMMENT 'Fingerprint',
    MODIFY  `sample` LONGTEXT NOT NULL COMMENT 'Sample',
    MODIFY  `first_seen` datetime (6) DEFAULT NULL COMMENT 'First seen time',
    MODIFY  `last_seen` datetime (6) DEFAULT NULL COMMENT 'Last seen time',
    MODIFY  `comments` LONGTEXT COMMENT 'Remark';


ALTER TABLE
  mysql_slow_query_review_history COMMENT 'Slow query details',
    MODIFY  `hostname_max` VARCHAR (64) NOT NULL COMMENT 'IP:Port',
    MODIFY  `client_max` VARCHAR (64) DEFAULT NULL COMMENT 'Client IP',
    MODIFY  `user_max` VARCHAR (64) NOT NULL COMMENT 'Username',
    MODIFY  `db_max` VARCHAR (64) DEFAULT NULL COMMENT 'Database',
    MODIFY  `checksum` CHAR (32) NOT NULL COMMENT 'Checksum',
    MODIFY  `sample` LONGTEXT NOT NULL COMMENT 'Sample',
    MODIFY  `ts_min` datetime (6) NOT NULL COMMENT 'First seen time',
    MODIFY  `ts_max` datetime (6) NOT NULL COMMENT 'Last seen time',
    MODIFY  `ts_cnt` FLOAT DEFAULT NULL COMMENT 'Count',
    MODIFY  `Query_time_sum` FLOAT DEFAULT NULL COMMENT 'Total query time',
    MODIFY  `Query_time_min` FLOAT DEFAULT NULL COMMENT 'Minimum query time',
    MODIFY  `Query_time_max` FLOAT DEFAULT NULL COMMENT 'Maximum query time',
    MODIFY  `Query_time_pct_95` FLOAT DEFAULT NULL COMMENT '95th percentile query time',
    MODIFY  `Query_time_stddev` FLOAT DEFAULT NULL COMMENT 'Average query time',
    MODIFY  `Query_time_median` FLOAT DEFAULT NULL COMMENT 'Median query time',
    MODIFY  `Lock_time_sum` FLOAT DEFAULT NULL COMMENT 'Total lock time',
    MODIFY  `Lock_time_min` FLOAT DEFAULT NULL COMMENT 'Minimum lock time',
    MODIFY  `Lock_time_max` FLOAT DEFAULT NULL COMMENT 'Maximum lock time',
    MODIFY  `Lock_time_pct_95` FLOAT DEFAULT NULL COMMENT '95th percentile lock time',
    MODIFY  `Lock_time_stddev` FLOAT DEFAULT NULL COMMENT 'Average lock time',
    MODIFY  `Lock_time_median` FLOAT DEFAULT NULL COMMENT 'Median lock time',
    MODIFY  `Rows_sent_sum` FLOAT DEFAULT NULL COMMENT 'Total rows sent',
    MODIFY  `Rows_sent_min` FLOAT DEFAULT NULL COMMENT 'Minimum rows sent',
    MODIFY  `Rows_sent_max` FLOAT DEFAULT NULL COMMENT 'Maximum rows sent',
    MODIFY  `Rows_sent_pct_95` FLOAT DEFAULT NULL COMMENT '95th percentile rows sent',
    MODIFY  `Rows_sent_stddev` FLOAT DEFAULT NULL COMMENT 'Average rows sent',
    MODIFY  `Rows_sent_median` FLOAT DEFAULT NULL COMMENT 'Median rows sent',
    MODIFY  `Rows_examined_sum` FLOAT DEFAULT NULL COMMENT 'Total rows examined',
    MODIFY  `Rows_examined_min` FLOAT DEFAULT NULL COMMENT 'Minimum rows examined',
    MODIFY  `Rows_examined_max` FLOAT DEFAULT NULL COMMENT 'Maximum rows examined',
    MODIFY  `Rows_examined_pct_95` FLOAT DEFAULT NULL COMMENT '95th percentile rows examined',
    MODIFY  `Rows_examined_stddev` FLOAT DEFAULT NULL COMMENT 'Standard rows examined',
    MODIFY  `Rows_examined_median` FLOAT DEFAULT NULL COMMENT 'Median rows examined',
    MODIFY  `Rows_affected_sum` FLOAT DEFAULT NULL COMMENT 'Total rows affected',
    MODIFY  `Rows_affected_min` FLOAT DEFAULT NULL COMMENT 'Minimum rows affected',
    MODIFY  `Rows_affected_max` FLOAT DEFAULT NULL COMMENT 'Maximum rows affected',
    MODIFY  `Rows_affected_pct_95` FLOAT DEFAULT NULL COMMENT '95th percentile rows affected',
    MODIFY  `Rows_affected_stddev` FLOAT DEFAULT NULL COMMENT 'Average rows affected',
    MODIFY  `Rows_affected_median` FLOAT DEFAULT NULL COMMENT 'Median rows affected',
    MODIFY  `Rows_read_sum` FLOAT DEFAULT NULL COMMENT 'Total rows read',
    MODIFY  `Rows_read_min` FLOAT DEFAULT NULL COMMENT 'Minimum rows read',
    MODIFY  `Rows_read_max` FLOAT DEFAULT NULL COMMENT 'Maximum rows read',
    MODIFY  `Rows_read_pct_95` FLOAT DEFAULT NULL COMMENT '95th percentile rows read',
    MODIFY  `Rows_read_stddev` FLOAT DEFAULT NULL COMMENT 'Average rows read',
    MODIFY  `Rows_read_median` FLOAT DEFAULT NULL COMMENT 'Median rows read';


ALTER TABLE
  param_history COMMENT 'Instance parameter change history - online editable dynamic parameter config',
    MODIFY  `instance_id` INT (11) NOT NULL COMMENT 'Instance ID',
    MODIFY  `variable_name` VARCHAR (64) NOT NULL COMMENT 'Parameter name',
    MODIFY  `old_var` VARCHAR (1024) NOT NULL COMMENT 'Parameter value before change',
    MODIFY  `new_var` VARCHAR (1024) NOT NULL COMMENT 'Parameter value after change',
    MODIFY  `set_sql` VARCHAR (1024) NOT NULL COMMENT 'Executed SQL for online config change',
    MODIFY  `user_name` VARCHAR (30) NOT NULL COMMENT 'Operator',
    MODIFY  `user_display` VARCHAR (50) NOT NULL COMMENT 'Operator display name',
    MODIFY  `update_time` datetime (6) NOT NULL COMMENT 'Update time';


ALTER TABLE
  param_template COMMENT 'Instance parameter template configuration',
    MODIFY  `db_type` VARCHAR (10) NOT NULL COMMENT 'Database type',
    MODIFY  `variable_name` VARCHAR (64) NOT NULL COMMENT 'Parameter name',
    MODIFY  `default_value` VARCHAR (1024) NOT NULL COMMENT 'Default parameter value',
    MODIFY  `editable` TINYINT (4) NOT NULL COMMENT 'Whether editable',
    MODIFY  `valid_values` VARCHAR (1024) NOT NULL COMMENT 'Valid values, range parameters [1-65535], value parameters [ON|OFF]',
    MODIFY  `description` VARCHAR (1024) NOT NULL COMMENT 'Parameter description',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time update';


ALTER TABLE
  query_log COMMENT 'Query log - records online SQL query logs',
    MODIFY  `instance_name` VARCHAR (50) NOT NULL COMMENT 'Instance name',
    MODIFY  `db_name` VARCHAR (64) NOT NULL COMMENT 'Database name',
    MODIFY  `sqllog` LONGTEXT NOT NULL COMMENT 'Executed SQL query',
    MODIFY  `effect_row` BIGINT (20) NOT NULL COMMENT 'Returned row count',
    MODIFY  `cost_time` VARCHAR (10) NOT NULL COMMENT 'Execution time',
    MODIFY  `username` VARCHAR (30) NOT NULL COMMENT 'Operator',
    MODIFY  `user_display` VARCHAR (50) NOT NULL COMMENT 'Operator display name',
    MODIFY  `priv_check` TINYINT (4) NOT NULL COMMENT 'Whether query privilege check is valid',
    MODIFY  `hit_rule` TINYINT (4) NOT NULL COMMENT 'Whether query hit masking rules',
    MODIFY  `masking` TINYINT (4) NOT NULL COMMENT 'Whether query result is properly masked',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Operation time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  query_privileges COMMENT 'Query privilege records - user permission relation table',
    MODIFY  `privilege_id` INT (11) NOT NULL AUTO_INCREMENT COMMENT 'Privilege ID',
    MODIFY  `user_name` VARCHAR (30) NOT NULL COMMENT 'Username',
    MODIFY  `user_display` VARCHAR (50) NOT NULL COMMENT 'Applicant display name',
    MODIFY  `instance_id` INT (11) NOT NULL COMMENT 'Instance ID',
    MODIFY  `table_name` VARCHAR (64) NOT NULL COMMENT 'Table',
    MODIFY  `db_name` VARCHAR (64) NOT NULL COMMENT 'Database',
    MODIFY  `valid_date` date NOT NULL COMMENT 'Valid until',
    MODIFY  `limit_num` INT (11) NOT NULL COMMENT 'Row limit',
    MODIFY  `priv_type` TINYINT (4) NOT NULL COMMENT 'Privilege level',
    MODIFY  `is_deleted` TINYINT (4) NOT NULL COMMENT 'Delete flag',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Application time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  query_privileges_apply COMMENT 'Query privilege application records',
    MODIFY  `group_id` INT (11) NOT NULL COMMENT 'Group ID',
    MODIFY  `group_name` VARCHAR (100) NOT NULL COMMENT 'Group name',
    MODIFY  `title` VARCHAR (50) NOT NULL COMMENT 'Application title',
    MODIFY  `user_name` VARCHAR (30) NOT NULL COMMENT 'Applicant',
    MODIFY  `user_display` VARCHAR (50) NOT NULL COMMENT 'Applicant display name',
    MODIFY  `instance_id` INT (11) NOT NULL COMMENT 'Instance ID',
    MODIFY  `db_list` LONGTEXT NOT NULL COMMENT 'Database list',
    MODIFY  `table_list` LONGTEXT NOT NULL COMMENT 'Table list',
    MODIFY  `valid_date` date NOT NULL COMMENT 'Valid until',
    MODIFY  `limit_num` INT (11) NOT NULL COMMENT 'Row limit',
    MODIFY  `priv_type` TINYINT (4) NOT NULL COMMENT 'Privilege type',
    MODIFY  `status` INT (11) NOT NULL COMMENT 'Review status',
    MODIFY  `audit_auth_groups` VARCHAR (255) NOT NULL COMMENT 'Review permission group list',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  resource_group COMMENT 'Resource group management - resource groups',
    MODIFY  `group_id` INT (11) NOT NULL AUTO_INCREMENT COMMENT 'Group ID',
    MODIFY  `group_name` VARCHAR (100) NOT NULL COMMENT 'Group name',
    MODIFY  `group_parent_id` BIGINT (20) NOT NULL COMMENT 'Parent ID',
    MODIFY  `group_sort` INT (11) NOT NULL COMMENT 'Sort order',
    MODIFY  `group_level` INT (11) NOT NULL COMMENT 'Level',
    MODIFY  `ding_webhook` VARCHAR (255) NOT NULL COMMENT 'DingTalk webhook URL',
    MODIFY  `is_deleted` TINYINT (4) NOT NULL COMMENT 'Whether deleted',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  resource_group_relations COMMENT 'Resource group object management - resource group relation table (users, instances, etc.)',
    MODIFY  `object_type` TINYINT (4) NOT NULL COMMENT 'Related object type',
    MODIFY  `object_id` INT (11) NOT NULL COMMENT 'Related object primary key ID',
    MODIFY  `object_name` VARCHAR (100) NOT NULL COMMENT 'Related object description, username or instance name',
    MODIFY  `group_id` INT (11) NOT NULL COMMENT 'Group ID',
    MODIFY  `group_name` VARCHAR (100) NOT NULL COMMENT 'Group name',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  sql_config COMMENT 'System configuration',
    MODIFY  `item` VARCHAR (50) NOT NULL COMMENT 'Config item',
    MODIFY  `value` VARCHAR (200) NOT NULL COMMENT 'Config item value',
    MODIFY  `description` VARCHAR (200) NOT NULL COMMENT 'Description';


ALTER TABLE
  sql_instance COMMENT 'Instance configuration - production instance settings',
    MODIFY  `instance_name` VARCHAR (50) NOT NULL COMMENT 'Instance name',
    MODIFY  `type` VARCHAR (6) NOT NULL COMMENT 'Master/slave role',
    MODIFY  `db_type` VARCHAR (10) NOT NULL COMMENT 'Database type',
    MODIFY  `host` VARCHAR (200) NOT NULL COMMENT 'Host',
    MODIFY  `port` INT (11) NOT NULL COMMENT 'Port',
    MODIFY  `user` VARCHAR (100) NOT NULL COMMENT 'User',
    MODIFY  `password` VARCHAR (300) NOT NULL COMMENT 'Password',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `update_time` datetime (6) NOT NULL COMMENT 'Update time';


ALTER TABLE
  sql_users COMMENT 'User management - user information',
    MODIFY  `password` VARCHAR (128) NOT NULL COMMENT 'Password',
    MODIFY  `last_login` datetime (6) DEFAULT NULL COMMENT 'Last login',
    MODIFY  `is_superuser` TINYINT (4) NOT NULL COMMENT 'Superuser status: 1 yes, 0 no',
    MODIFY  `username` VARCHAR (150) NOT NULL COMMENT 'Username',
    MODIFY  `first_name` VARCHAR (30) NOT NULL COMMENT 'First name, empty by default',
    MODIFY  `last_name` VARCHAR (150) NOT NULL COMMENT 'Last name, empty by default',
    MODIFY  `email` VARCHAR (254) NOT NULL COMMENT 'Email address',
    MODIFY  `is_staff` TINYINT (4) NOT NULL COMMENT 'Staff status (can manage Django admin): 1 yes, 0 no',
    MODIFY  `is_active` TINYINT (4) NOT NULL COMMENT 'Active status (disabled user flag): 1 yes, 0 no',
    MODIFY  `date_joined` datetime (6) NOT NULL COMMENT 'Join date (first login time)',
    MODIFY  `display` VARCHAR (50) NOT NULL COMMENT 'Display name',
    MODIFY  `failed_login_count` INT (11) NOT NULL COMMENT 'Failed login count',
    MODIFY  `last_login_failed_at` datetime DEFAULT NULL COMMENT 'Last failed login time';


ALTER TABLE
  sql_workflow COMMENT 'SQL workflow - stores base information for SQL release workflows',
    MODIFY  `workflow_name` VARCHAR (50) NOT NULL COMMENT 'Workflow content',
    MODIFY  `group_id` INT (11) NOT NULL COMMENT 'Group ID',
    MODIFY  `group_name` VARCHAR (100) NOT NULL COMMENT 'Group name',
    MODIFY  `instance_id` INT (11) NOT NULL COMMENT 'Instance ID',
    MODIFY  `db_name` VARCHAR (64) NOT NULL COMMENT 'Database',
    MODIFY  `engineer` VARCHAR (30) NOT NULL COMMENT 'Initiator',
    MODIFY  `engineer_display` VARCHAR (50) NOT NULL COMMENT 'Initiator display name',
    MODIFY  `audit_auth_groups` VARCHAR (255) NOT NULL COMMENT 'Review permission group list',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `finish_time` datetime (6) DEFAULT NULL COMMENT 'Finish time',
    MODIFY  `status` VARCHAR (50) NOT NULL COMMENT 'Workflow status',
    MODIFY  `is_backup` TINYINT (4) NOT NULL COMMENT 'Whether backup is enabled',
    MODIFY  `is_manual` TINYINT (4) NOT NULL COMMENT 'Whether native execution is used',
    MODIFY  `syntax_type` TINYINT (4) NOT NULL COMMENT 'Workflow type: 1 DDL, 2 DML';


ALTER TABLE
  sql_workflow_content COMMENT 'SQL workflow content - stores SQL/review/execution details; can be archived or cleaned regularly, and can be compressed with alter table sql_workflow_content row_format=compressed',
    MODIFY  `workflow_id` INT (11) NOT NULL COMMENT 'SQL workflow ID',
    MODIFY  `sql_content` LONGTEXT NOT NULL COMMENT 'Submitted SQL text',
    MODIFY  `review_content` LONGTEXT NOT NULL COMMENT 'Auto-review content in JSON format',
    MODIFY  `execute_result` LONGTEXT NOT NULL COMMENT 'Execution result in JSON format';


ALTER TABLE
  workflow_audit COMMENT 'Workflow audit list - workflow review status table',
    MODIFY  `group_id` INT (11) NOT NULL COMMENT 'Group ID',
    MODIFY  `group_name` VARCHAR (100) NOT NULL COMMENT 'Group name',
    MODIFY  `workflow_id` BIGINT (20) NOT NULL COMMENT 'Related business ID',
    MODIFY  `workflow_type` TINYINT (4) NOT NULL COMMENT 'Application type',
    MODIFY  `workflow_title` VARCHAR (50) NOT NULL COMMENT 'Application title',
    MODIFY  `workflow_remark` VARCHAR (140) NOT NULL COMMENT 'Application remark',
    MODIFY  `audit_auth_groups` VARCHAR (255) NOT NULL COMMENT 'Review permission group list',
    MODIFY  `current_audit` VARCHAR (20) NOT NULL COMMENT 'Current review permission group',
    MODIFY  `next_audit` VARCHAR (20) NOT NULL COMMENT 'Next review permission group',
    MODIFY  `current_status` TINYINT (4) NOT NULL COMMENT 'Review status',
    MODIFY  `create_user` VARCHAR (30) NOT NULL COMMENT 'Applicant',
    MODIFY  `create_user_display` VARCHAR (50) NOT NULL COMMENT 'Applicant display name',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Application time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  workflow_audit_detail COMMENT 'Workflow audit detail - review detail table',
    MODIFY  `audit_id` INT (11) NOT NULL COMMENT 'Audit main table ID',
    MODIFY  `audit_user` VARCHAR (30) NOT NULL COMMENT 'Reviewer',
    MODIFY  `audit_time` datetime (6) NOT NULL COMMENT 'Review time',
    MODIFY  `audit_status` TINYINT (4) NOT NULL COMMENT 'Review status',
    MODIFY  `remark` VARCHAR (140) NOT NULL COMMENT 'Review remark',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  workflow_audit_setting COMMENT 'Approval workflow configuration - approval config table',
    MODIFY  `group_id` INT (11) NOT NULL COMMENT 'Group ID',
    MODIFY  `group_name` VARCHAR (100) NOT NULL COMMENT 'Group name',
    MODIFY  `workflow_type` TINYINT (4) NOT NULL COMMENT 'Approval type',
    MODIFY  `audit_auth_groups` VARCHAR (255) NOT NULL COMMENT 'Approval permission group list',
    MODIFY  `create_time` datetime (6) NOT NULL COMMENT 'Create time',
    MODIFY  `sys_time` datetime (6) NOT NULL COMMENT 'System time';


ALTER TABLE
  workflow_log COMMENT 'Workflow log',
    MODIFY  `audit_id` BIGINT (20) NOT NULL COMMENT 'Workflow audit ID',
    MODIFY  `operation_type` TINYINT (4) NOT NULL COMMENT 'Operation type, 0 submit/pending review, 1 approved, 2 rejected, 3 canceled/cancel execution, 4 scheduled execution, 5 execute workflow, 6 execution finished',
    MODIFY  `operation_type_desc` CHAR (10) NOT NULL COMMENT 'Operation type description',
    MODIFY  `operation_info` VARCHAR (200) NOT NULL COMMENT 'Operation info',
    MODIFY  `operator` VARCHAR (30) NOT NULL COMMENT 'Operator',
    MODIFY  `operator_display` VARCHAR (50) NOT NULL COMMENT 'Operator display name',
    MODIFY  `operation_time` datetime (6) NOT NULL COMMENT 'Operation time';
