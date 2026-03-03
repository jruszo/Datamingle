-- Add workflow log table
CREATE TABLE `workflow_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `audit_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'Workflow audit ID',
  `operation_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Operation type: 0 submit/pending review, 1 approved, 2 rejected, 3 canceled/cancel execution, 4 scheduled execution, 5 execute workflow, 6 execution finished',
  `operation_type_desc` varchar(64) NOT NULL DEFAULT '' COMMENT 'Operation type description',
  `operation_info` varchar(200) NOT NULL DEFAULT '' COMMENT 'Operation info',
  `operator` varchar(30) NOT NULL DEFAULT '' COMMENT 'Operator',
  `operator_display` varchar(50) NOT NULL DEFAULT '' COMMENT 'Operator display name',
  `operation_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'Operation time',
  PRIMARY KEY (`id`),
  index idx_audit_id(audit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Add menu permissions
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Database Review', @content_type_id, 'menu_themis');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Instance Management', @content_type_id, 'menu_instance');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Binlog2SQL', @content_type_id, 'menu_binlog2sql');
