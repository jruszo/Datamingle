-- Add login failure information
alter table sql_users
  add failed_login_count tinyint not null default 0 comment 'Failure count',
  add last_login_failed_at timestamp comment 'Last failed login time';

-- Rename resource tables
rename table sql_group to resource_group;
rename table sql_group_relations to resource_group_relations;

-- Replace django_apscheduler with django_q
drop table django_apscheduler_djangojobexecution;
drop table django_apscheduler_djangojob;
