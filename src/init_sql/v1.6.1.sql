-- Expand database type fields
alter table sql_instance modify db_type varchar(20) not null default '' comment 'Database type';
alter table param_template modify db_type varchar(20) not null default '' comment 'Database type';
