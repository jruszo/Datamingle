-- Retired legacy connectivity objects removed.

-- Add WeCom configuration
alter table resource_group add qywx_webhook varchar(255) not null default '' comment 'WeCom webhook URL';
