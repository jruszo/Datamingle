-- Add data dictionary permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu Data Dictionary', @content_type_id, 'menu_data_dictionary');
