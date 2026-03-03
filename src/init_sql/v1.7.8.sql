-- Increase workflow remark length
alter table workflow_audit_detail modify remark varchar(1000) NOT NULL DEFAULT '' COMMENT 'Review remark';
alter table workflow_log modify operation_info varchar(1000) NOT NULL DEFAULT '' COMMENT 'Operation info';

-- Add Feishu information
alter table sql_users add feishu_open_id varchar(64) not null default '' comment 'Feishu OpenID';
alter table resource_group add feishu_webhook varchar(255) not null default '' comment 'Feishu webhook URL';
