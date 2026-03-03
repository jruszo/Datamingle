-- Update workflow statuses
UPDATE sql_workflow SET status = 'workflow_finish' WHERE status='Workflow finished normally';
UPDATE sql_workflow SET status = 'workflow_abort' WHERE status='Manually terminated workflow';
UPDATE sql_workflow SET status = 'workflow_manreviewing' WHERE status='Waiting for reviewer approval';
UPDATE sql_workflow SET status = 'workflow_review_pass' WHERE status='Review passed';
UPDATE sql_workflow SET status = 'workflow_timingtask' WHERE status='Scheduled execution';
UPDATE sql_workflow SET status = 'workflow_executing' WHERE status='Executing';
UPDATE sql_workflow SET status = 'workflow_autoreviewwrong' WHERE status='Automatic review failed';
UPDATE sql_workflow SET status = 'workflow_exception' WHERE status='Execution exception';

-- Change display column to not null
alter table sql_users modify display varchar(50) not null default '' comment 'Display name';
