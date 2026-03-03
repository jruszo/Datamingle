-- Update menu code for tools plugin
UPDATE auth_permission SET codename='menu_tools' WHERE codename='menu_menu_tools';

-- Add demand URL to SQL release workflows
ALTER TABLE sql_workflow ADD demand_url varchar(500) NOT NULL DEFAULT '' COMMENT 'Demand URL';

-- Add transaction viewing permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT IGNORE INTO auth_permission (name, content_type_id, codename) VALUES ('View transaction information', @content_type_id, 'trx_view');
