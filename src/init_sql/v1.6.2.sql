-- Resource group and user association table
CREATE TABLE `resource_group_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary key',
  `resource_group_id` int(11) NOT NULL COMMENT 'Resource group',
  `user_id` int(11) NOT NULL COMMENT 'User',
  `create_time` datetime(6) NOT NULL COMMENT 'Created time',
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  UNIQUE uniq_resource_group_id_instance_id(`resource_group_id`,`user_id`),
  CONSTRAINT `fk_resource_group_user_resource_group` FOREIGN KEY (`resource_group_id`) REFERENCES `resource_group` (`group_id`),
  CONSTRAINT `fk_resource_group_user` FOREIGN KEY (`user_id`) REFERENCES `sql_users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Resource group and instance association table
CREATE TABLE `resource_group_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary key',
  `resource_group_id` int(11) NOT NULL COMMENT 'Resource group',
  `instance_id` int(11) NOT NULL COMMENT 'Instance',
  `create_time` datetime(6) NOT NULL COMMENT 'Created time',
  PRIMARY KEY (`id`),
  KEY `idx_instance_id` (`instance_id`),
  UNIQUE uniq_resource_group_id_instance_id(`resource_group_id`,`instance_id`),
  CONSTRAINT `fk_resource_group_instance_resource_group` FOREIGN KEY (`resource_group_id`) REFERENCES `resource_group` (`group_id`),
  CONSTRAINT `fk_resource_group_instance` FOREIGN KEY (`instance_id`) REFERENCES `sql_instance` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Data cleanup
set foreign_key_checks = 0;
-- User relation data
insert into resource_group_user (resource_group_id, user_id, create_time)
select group_id,object_id,create_time from resource_group_relations where object_type=0;
-- Instance relation data
insert into resource_group_instance (resource_group_id, instance_id, create_time)
select group_id,object_id,create_time from resource_group_relations where object_type=1;
set foreign_key_checks = 1;

-- Drop old table
drop table resource_group_relations;

-- Add executable time range for SQL release workflows
ALTER TABLE sql_workflow
  ADD run_date_start datetime(6) DEFAULT NULL COMMENT 'Executable start time',
  ADD run_date_end datetime(6) DEFAULT NULL COMMENT 'Executable end time';

-- Add default charset information to instance configuration
ALTER TABLE sql_instance
  ADD `charset` varchar(20) DEFAULT NULL COMMENT 'Charset' after `password`;
