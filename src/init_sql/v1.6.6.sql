-- Add query statement favorite/pin feature
alter table query_log
  add favorite tinyint not null default 0 comment 'Is favorited',
  add alias varchar(100) not null default '' comment 'Statement identifier/alias';
