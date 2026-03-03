-- Remove Themis permission
set @perm_id=(select id from auth_permission where codename='menu_themis');
delete from auth_group_permissions where permission_id=@perm_id;
delete from sql_users_user_permissions where permission_id=@perm_id;
delete from auth_permission where codename='menu_themis';
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
-- Add instance account management permission and update menu permission info
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Manage Instance Accounts', @content_type_id, 'instance_account_manage');
UPDATE auth_permission set name='Menu Instance Account Management',codename='menu_instance_account' where codename='menu_instance_user';
-- Add instance database permission
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Database Management', @content_type_id, 'menu_database');
-- Add resource-group-level query permission
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Can query all instances in the current resource group', @content_type_id, 'query_resource_group_instance');
-- Add tools plugin permission
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Tools Plugin', @content_type_id, 'menu_menu_tools');

-- Add DingTalk user ID
alter table sql_users
  add ding_user_id varchar(64) default null comment 'DingTalk user_id';


-- Add instance account table
CREATE TABLE `instance_account` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user` varchar(128) NOT NULL COMMENT 'Account',
  `host` varchar(64) NOT NULL COMMENT 'Host',
  `password` varchar(128) NOT NULL COMMENT 'Password',
  `remark` varchar(255) NOT NULL COMMENT 'Remark',
  `sys_time` datetime(6) NOT NULL COMMENT 'System time',
  `instance_id` int(11) NOT NULL COMMENT 'Instance',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_instance_id_user_host` (`instance_id`,`user`,`host`),
  CONSTRAINT `fk_account_sql_instance_id` FOREIGN KEY (`instance_id`) REFERENCES `sql_instance` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- Add instance database table
CREATE TABLE `instance_database` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `db_name` varchar(128) NOT NULL COMMENT 'Database name',
  `owner` varchar(30) NOT NULL COMMENT 'Owner',
  `owner_display` varchar(50) NOT NULL DEFAULT 'Owner display name',
  `remark` varchar(255) NOT NULL COMMENT 'Remark',
  `sys_time` datetime(6) NOT NULL COMMENT 'System time',
  `instance_id` int(11) NOT NULL COMMENT 'Instance',
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_database_sql_instance_id` FOREIGN KEY (`instance_id`) REFERENCES `sql_instance` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

