-- Add export data dictionary permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT IGNORE INTO auth_permission (name, content_type_id, codename) VALUES ('Export Data Dictionary', @content_type_id, 'data_dictionary_export');
