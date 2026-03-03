-- Update Alibaba Cloud configuration table
alter table aliyun_rds_config add is_enable tinyint not null default 0 comment 'Enabled or not';
