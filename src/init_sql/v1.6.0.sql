-- Add Oracle instance-related fields
ALTER TABLE sql_instance
  ADD `sid` varchar(50) DEFAULT NULL COMMENT 'Oracle sid' AFTER password,
  ADD `service_name` varchar(50) DEFAULT NULL COMMENT 'Oracle Service name' AFTER password;

-- Rename field
ALTER TABLE param_history CHANGE update_time create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Parameter modification time';

-- Change charset to utf8mb4 (skip this statement if it is already utf8mb4)
ALTER TABLE sql_workflow_content
  modify `sql_content` longtext CHARACTER SET utf8mb4 NOT NULL COMMENT 'Submitted SQL text',
  modify `review_content` longtext CHARACTER SET utf8mb4 NOT NULL COMMENT 'Auto-review content in JSON format',
  modify `execute_result` longtext CHARACTER SET utf8mb4 NOT NULL COMMENT 'Execution result in JSON format';

ALTER TABLE query_log
  modify `sqllog` longtext CHARACTER SET utf8mb4 NOT NULL COMMENT 'Executed SQL query';

-- Add instance tag configuration
CREATE TABLE `sql_instance_tag` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Tag ID',
  `tag_code` varchar(20) NOT NULL COMMENT 'Tag code',
  `tag_name` varchar(20) NOT NULL COMMENT 'Tag name',
  `active` tinyint(1) NOT NULL COMMENT 'Active status',
  `create_time` datetime(6) NOT NULL COMMENT 'Created time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_tag_code` (`tag_code`),
  UNIQUE KEY `uniq_tag_name` (`tag_name`)
) COMMENT 'Instance tag configuration' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `sql_instance_tag_relations` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Relation ID',
  `instance_id` int(11) NOT NULL COMMENT 'Related instance ID',
  `instance_tag_id` int(11) NOT NULL COMMENT 'Related tag ID',
  `active` tinyint(1) NOT NULL COMMENT 'Active status',
  `create_time` datetime(6) NOT NULL COMMENT 'Created time',
  PRIMARY KEY (`id`),
  KEY `idx_instance_tag_id` (`instance_tag_id`),
  UNIQUE KEY `uniq_instance_id_instance_tag_id` (`instance_id`,`instance_tag_id`),
  CONSTRAINT `fk_itr_instance` FOREIGN KEY (`instance_id`) REFERENCES `sql_instance` (`id`),
  CONSTRAINT `fk_itr_instance_tag` FOREIGN KEY (`instance_tag_id`) REFERENCES `sql_instance_tag` (`id`)
) COMMENT 'Instance tag relation' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Initialize tag data
INSERT INTO sql_instance_tag (id, tag_code, tag_name, active, create_time) VALUES (1, 'can_write', 'Supports release', 1, '2019-05-03 00:00:00.000000');
INSERT INTO sql_instance_tag (id, tag_code, tag_name, active, create_time) VALUES (2, 'can_read', 'Supports query', 1, '2019-05-03 00:00:00.000000');

-- Add tags for existing master/slave data
insert into sql_instance_tag_relations (instance_id, instance_tag_id, active, create_time)
select id,1,1,now() from sql_instance where type='master';
insert into sql_instance_tag_relations (instance_id, instance_tag_id, active, create_time)
select id,2,1,now() from sql_instance where type='slave';

set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Can query all instances', @content_type_id, 'query_all_instances');
