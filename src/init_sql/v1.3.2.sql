-- Update query log table
ALTER TABLE query_log
  ADD priv_check TINYINT NOT NULL DEFAULT 0
COMMENT 'Whether query privilege check is valid: 1=valid, 2=skipped'
  AFTER user_display,
  ADD hit_rule TINYINT NOT NULL DEFAULT 0
COMMENT 'Whether query hit masking rules: 0=unknown, 1=hit, 2=not hit'
  AFTER priv_check,
  ADD masking TINYINT NOT NULL DEFAULT 0
COMMENT 'Whether query result is properly masked: 1=yes, 2=no'
  AFTER hit_rule;

-- Add menu permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu SchemaSync', @content_type_id, 'menu_schemasync');
