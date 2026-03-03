-- Add OpenAPI menu permission
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('Menu OpenAPI', @content_type_id, 'menu_openapi');

-- Add 2FA configuration table
CREATE TABLE `2fa_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(200) NOT NULL,
  `auth_type` varchar(128) NOT NULL,
  `secret_key` varchar(256) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  CONSTRAINT `2fa_config_user_id_a0d1d7c2_fk_sql_users_id` FOREIGN KEY (`user_id`) REFERENCES `sql_users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
