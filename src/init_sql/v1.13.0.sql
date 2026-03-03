-- Add fields to workflow table
ALTER TABLE sql_workflow
ADD COLUMN export_format VARCHAR(10) DEFAULT NULL,
ADD COLUMN is_offline_export TINYINT(1) NOT NULL,
ADD COLUMN file_name VARCHAR(255) DEFAULT NULL;

-- Add permissions
SET @content_type_id=(SELECT id FROM django_content_type WHERE app_label='sql' AND model='permission');
INSERT IGNORE INTO auth_permission (name, content_type_id, codename) 
VALUES
  ('Offline download permission', @content_type_id, 'offline_download'),
  ('Menu Data Export', @content_type_id, 'menu_sqlexportworkflow'),
  ('Submit data export', @content_type_id, 'sqlexport_submit');
