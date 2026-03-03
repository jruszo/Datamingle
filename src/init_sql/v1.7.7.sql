-- Modify many-to-many intermediate tables
alter table resource_group_user
  rename to sql_users_resource_group,
  drop create_time,
  change user_id users_id int(11) NOT NULL COMMENT 'User',
  change resource_group_id resourcegroup_id int(11) NOT NULL COMMENT 'Resource group';

alter table resource_group_instance
  rename to sql_instance_resource_group,
  drop create_time,
  change resource_group_id resourcegroup_id int(11) NOT NULL COMMENT 'Resource group';

alter table sql_instance_tag_relations
  rename to sql_instance_instance_tag,
  drop `active`,
  drop create_time,
  change instance_tag_id instancetag_id int(11) NOT NULL COMMENT 'Related tag ID';

-- Add default database field to instance configuration table
ALTER TABLE sql_instance ADD `db_name` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'Database';
