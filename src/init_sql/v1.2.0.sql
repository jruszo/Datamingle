-- ==============================================================
-- Master/slave table merge related changes
-- Rename table names
rename table sql_master_config to sql_instance;

-- Field definition changes
alter table sql_instance
  change cluster_name instance_name varchar(50) NOT NULL ,
  change master_host host varchar(200) NOT NULL ,
  change master_port port int NOT NULL,
  change master_user user varchar(100) NOT NULL,
  change master_password password varchar(300) NOT NULL,
  add type char(6) NOT NULL after instance_name,
  add db_type varchar(10) NOT NULL after type;

-- Update information
update sql_instance set db_type='mysql',type='master';

-- Add slave data to instance table (if master/slave had the same instance_name, fix it first and update related tables)
insert into sql_instance (instance_name, db_type, type, host, port, user, password, create_time, update_time)
  select
    cluster_name,
    'mysql',
    'slave',
    slave_host,
    slave_port,
    slave_user,
    slave_password,
    create_time,
    update_time
  from sql_slave_config;

-- Rebuild resource-group instance relations with unified instance type (no master/slave distinction)
update sql_group_relations a
  join sql_instance b on a.object_name = b.instance_name
set a.object_id = b.id, a.object_type = 1
where a.object_type in (2, 3);

-- Change related field information
alter table sql_workflow change cluster_name instance_name varchar(50) NOT NULL ;
alter table query_privileges_apply change cluster_name instance_name varchar(50) NOT NULL ;
alter table query_privileges change cluster_name instance_name varchar(50) NOT NULL ;
alter table query_log change cluster_name instance_name varchar(50) NOT NULL ;
alter table data_masking_columns change cluster_name instance_name varchar(50) NOT NULL ;
alter table aliyun_rds_config change cluster_name instance_name varchar(50) NOT NULL ;



-- ==============================================================
-- Permission management related changes
-- Remove role field
alter table sql_users drop role;

-- Change field information
alter table sql_workflow
  change review_man audit_auth_groups varchar(255) NOT NULL;
alter table query_privileges_apply
  change audit_users audit_auth_groups varchar(255) NOT NULL;
alter table workflow_audit_setting
  change audit_users audit_auth_groups varchar(255) NOT NULL;
alter table workflow_audit
  change audit_users audit_auth_groups varchar(255) NOT NULL,
  change current_audit_user  current_audit varchar(20) NOT NULL,
  change next_audit_user next_audit varchar(20) NOT NULL;

-- Clear permission and permission-group data
set foreign_key_checks =0;
truncate table auth_group_permissions;
truncate table sql_users_user_permissions;
truncate table auth_permission;
truncate table auth_group;
truncate table sql_users_groups;
set foreign_key_checks =1;

-- Insert permissions and default permission group
INSERT INTO auth_group (id, name) VALUES (1, 'Default Group'); -- New users are associated with group id=1 by default; do not delete
INSERT INTO django_content_type (app_label, model) VALUES ('sql', 'permission');
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Dashboard', @content_type_id, 'menu_dashboard');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu SQL Release', @content_type_id, 'menu_sqlworkflow');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu SQL Query', @content_type_id, 'menu_query');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu MySQL Query', @content_type_id, 'menu_sqlquery');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Query Permission Request', @content_type_id, 'menu_queryapplylist');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu SQL Optimization', @content_type_id, 'menu_sqloptimize');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Optimization Tools', @content_type_id, 'menu_sqladvisor');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Slow Query Log', @content_type_id, 'menu_slowquery');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Session Management', @content_type_id, 'menu_dbdiagnostic');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Menu System Management', @content_type_id, 'menu_system');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Menu Documentation', @content_type_id, 'menu_document');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Submit SQL release workflow', @content_type_id, 'sql_submit');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Review SQL release workflow', @content_type_id, 'sql_review');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Execute SQL release workflow', @content_type_id, 'sql_execute');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Execute SQLAdvisor', @content_type_id, 'optimize_sqladvisor');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Execute SQLTuning', @content_type_id, 'optimize_sqltuning');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Apply for query permission', @content_type_id, 'query_applypriv');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Manage query permissions', @content_type_id, 'query_mgtpriv');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Review query permissions', @content_type_id, 'query_review');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Submit SQL query', @content_type_id, 'query_submit');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'View sessions', @content_type_id, 'process_view');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'Terminate sessions', @content_type_id, 'process_kill');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'View tablespace', @content_type_id, 'tablespace_view');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ( 'View lock information', @content_type_id, 'trxandlocks_view');

-- Grant default permissions to the default group (all permissions by default, adjust as needed)
insert into auth_group_permissions (group_id, permission_id)
select 1,id from auth_permission;

-- Associate all users with the default group
insert into sql_users_groups(users_id, group_id)
select id,1 from sql_users;

-- ==============================================================
-- Compatible with pt-query-digest 3.0.11
alter table mysql_slow_query_review modify `checksum` CHAR(32) NOT NULL;
alter table mysql_slow_query_review_history modify `checksum` CHAR(32) NOT NULL;

-- ==============================================================
-- Normalize username length
alter table sql_workflow modify engineer varchar(30) not null;
alter table workflow_audit modify create_user varchar(30) not null;
alter table workflow_audit_detail modify audit_user varchar(30) not null;
