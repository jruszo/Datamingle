-- Add login audit log
CREATE TABLE `audit_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `user_id` int(11) DEFAULT NULL COMMENT 'User ID',
  `user_name` varchar(255) DEFAULT NULL COMMENT 'User name',
  `ip` varchar(255) DEFAULT NULL COMMENT 'Login IP',
  `action` varchar(255) DEFAULT NULL COMMENT 'Action',
  `action_time` datetime(6) NOT NULL COMMENT 'Operation time',
  PRIMARY KEY (`id`),
  KEY `idx_username` (`user_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Login audit log table';

-- Add my2sql menu permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu My2SQL', @content_type_id, 'menu_my2sql');

-- SSH tunnel feature update
ALTER TABLE `ssh_tunnel` ADD COLUMN pkey longtext NULL AFTER password;

-- Audit feature enhancements
alter table audit_log change `ip` `extra_info` longtext DEFAULT NULL COMMENT 'Additional info'; 
alter table audit_log add `user_display` varchar(50) DEFAULT NULL COMMENT 'User display name'; 

set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
insert IGNORE INTO auth_permission (name, content_type_id, codename) VALUES
('Audit permission', @content_type_id, 'audit_user');

-- Online query download permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
insert IGNORE INTO auth_permission (name, content_type_id, codename) VALUES
('Online query download permission', @content_type_id, 'query_download');

-- Add mode field to instance config table for Redis instances; set default value for historical data
alter table sql_instance add column `mode` varchar(10) DEFAULT '' after `db_type`;
update sql_instance set mode='standalone' where db_type='redis';
