-- Add resource-group-level query permission (missing in v1.7.0 model; required for new installations)
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT IGNORE INTO auth_permission (name, content_type_id, codename) VALUES ('Can query all instances in the current resource group', @content_type_id, 'query_resource_group_instance');

-- Store WeCom user ID
alter table sql_users add wx_user_id varchar(64) default null comment 'WeCom UserID';

-- Drop Alibaba Cloud AK configuration table, move to system configuration, and enlarge system config field length
drop table aliyun_access_key;
alter table sql_config modify `item` varchar(200) NOT NULL comment 'Config item',
 modify `value` varchar(500) NOT NULL DEFAULT '' comment 'Config item value';

-- Use django-mirage-field to encrypt instance information and increase field length
alter table sql_instance modify `user` varchar(200) DEFAULT NULL COMMENT 'Username';
