-- Add archive-related permissions
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
insert IGNORE INTO auth_permission (name, content_type_id, codename) VALUES
('Menu Data Archive', @content_type_id, 'menu_archive'),
('Submit archive request', @content_type_id, 'archive_apply'),
('Review archive request', @content_type_id, 'archive_audit'),
('Manage archive request', @content_type_id, 'archive_mgt');

-- Archive configuration table
CREATE TABLE `archive_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary key',
  `title` varchar(50) NOT NULL COMMENT 'Archive configuration description',
  `resource_group_id` int(11) NOT NULL COMMENT 'Resource group',
  `audit_auth_groups` varchar(255) NOT NULL COMMENT 'Review permission group list',
  `src_instance_id` int(11) NOT NULL COMMENT 'Source instance',
  `src_db_name` varchar(64) NOT NULL COMMENT 'Source database',
  `src_table_name` varchar(64) NOT NULL COMMENT 'Source table',
  `dest_instance_id` int(11) DEFAULT NULL COMMENT 'Destination instance',
  `dest_db_name` varchar(64) DEFAULT NULL COMMENT 'Destination database',
  `dest_table_name` varchar(64) DEFAULT NULL COMMENT 'Destination table',
  `condition` varchar(1000) NOT NULL COMMENT 'Archive condition (WHERE clause)',
  `mode` varchar(10) NOT NULL COMMENT 'Archive mode',
  `no_delete` tinyint(1) NOT NULL COMMENT 'Whether to keep source data',
  `sleep` int(11) NOT NULL COMMENT 'Sleep seconds after each archive LIMIT batch',
  `status` int(11) NOT NULL COMMENT 'Review status',
  `state` tinyint(1) NOT NULL COMMENT 'Whether archive is enabled',
  `user_name` varchar(30) NOT NULL COMMENT 'Applicant',
  `user_display` varchar(50) NOT NULL COMMENT 'Applicant display name',
  `create_time` datetime(6) NOT NULL COMMENT 'Create time',
  `last_archive_time` datetime(6) DEFAULT NULL COMMENT 'Last archive time',
  `sys_time` datetime(6) NOT NULL COMMENT 'System time update',
  PRIMARY KEY (`id`),
  KEY `idx_dest_instance_id` (`dest_instance_id`),
  KEY `idx_resource_group_id` (`resource_group_id`),
  KEY `idx_src_instance_id` (`src_instance_id`),
  CONSTRAINT `fk_archive_dest_instance_id` FOREIGN KEY (`dest_instance_id`) REFERENCES `sql_instance` (`id`),
  CONSTRAINT `fk_archive_resource_id` FOREIGN KEY (`resource_group_id`) REFERENCES `resource_group` (`group_id`),
  CONSTRAINT `fk_archive_src_instance_id` FOREIGN KEY (`src_instance_id`) REFERENCES `sql_instance` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'Archive configuration table';

-- Archive log table
CREATE TABLE `archive_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary key',
  `archive_id` int(11) NOT NULL COMMENT 'Archive configuration ID',
  `cmd` varchar(2000) NOT NULL COMMENT 'Archive command',
  `condition` varchar(1000) NOT NULL COMMENT 'Archive condition',
  `mode` varchar(10) NOT NULL COMMENT 'Archive mode',
  `no_delete` tinyint(1) NOT NULL COMMENT 'Whether to keep source data',
  `sleep` int(11) NOT NULL COMMENT 'Sleep seconds after each archive LIMIT batch',
  `select_cnt` int(11) NOT NULL COMMENT 'Selected row count',
  `insert_cnt` int(11) NOT NULL COMMENT 'Inserted row count',
  `delete_cnt` int(11) NOT NULL COMMENT 'Deleted row count',
  `statistics` longtext NOT NULL COMMENT 'Archive statistics log',
  `success` tinyint(1) NOT NULL COMMENT 'Whether archive succeeded',
  `error_info` longtext NOT NULL COMMENT 'Error info',
  `start_time` datetime(6) NOT NULL COMMENT 'Start time',
  `end_time` datetime(6) NOT NULL COMMENT 'End time',
  `sys_time` datetime(6) NOT NULL COMMENT 'System time update',
  PRIMARY KEY (`id`),
  KEY `idx_archive_id` (`archive_id`),
  CONSTRAINT `fk_archive_config_id` FOREIGN KEY (`archive_id`) REFERENCES `archive_config` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'Archive log table';
