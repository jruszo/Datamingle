-- Used to delete non-custom permissions. If model-level permission management is not needed, run this script to keep only custom permissions.
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');

-- delete auth_group_permissions
delete a
from auth_group_permissions a
       join auth_permission b on a.permission_id = b.id
where (b.content_type_id <> @content_type_id or
       (b.content_type_id = @content_type_id and
        codename in ('add_permission', 'change_permission', 'delete_permission')));

-- delete sql_users_user_permissions
delete a
from sql_users_user_permissions a
       join auth_permission b on a.permission_id = b.id
where (b.content_type_id <> @content_type_id or
       (b.content_type_id = @content_type_id and
        codename in ('add_permission', 'change_permission', 'delete_permission')));

-- delete auth_permission
delete
from auth_permission
where (content_type_id <> @content_type_id or
       (content_type_id = @content_type_id and
        codename in ('add_permission', 'change_permission', 'delete_permission')));
