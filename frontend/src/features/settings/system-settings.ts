import type { SystemSettings, SystemSettingsOptions, SystemSettingsValue } from '@/lib/api'

export type SystemSettingsFieldInput =
  | 'text'
  | 'password'
  | 'number'
  | 'checkbox'
  | 'textarea'
  | 'select'
  | 'multiselect'

export type SystemSettingsFieldDefinition = {
  key: string
  label: string
  input: SystemSettingsFieldInput
  description?: string
  placeholder?: string
  optionSource?: keyof SystemSettingsOptions
  defaultValue?: SystemSettingsValue
  rows?: number
  showWhen?: {
    key: string
    equals: string | boolean
  }
}

export type SystemSettingsSectionDefinition = {
  id: string
  title: string
  description: string
  testAction?: 'goInception' | 'email' | 'storage'
  fields: SystemSettingsFieldDefinition[]
}

const DEFAULT_QUERY_TEMPLATE =
  'You are an engineer familiar with {{db_type}}. I will give you basic information and requirements. Generate one query for me. Do not return comments or numbering. Return only the query: {{table_schema}} \n {{user_input}}'

export const systemSettingsSections: SystemSettingsSectionDefinition[] = [
  {
    id: 'sql-review',
    title: 'SQL Review Engine',
    description: 'Configure goInception and rollback backup connectivity used for SQL review and rollback generation.',
    testAction: 'goInception',
    fields: [
      { key: 'go_inception_host', label: 'goInception host', input: 'text', placeholder: 'goInception host' },
      { key: 'go_inception_port', label: 'goInception port', input: 'number', placeholder: 'goInception port' },
      { key: 'go_inception_user', label: 'goInception user', input: 'text', placeholder: 'Optional if auth is disabled' },
      { key: 'go_inception_password', label: 'goInception password', input: 'password', placeholder: 'Optional if auth is disabled' },
      { key: 'inception_remote_backup_host', label: 'Backup host', input: 'text', placeholder: 'Backup database host' },
      { key: 'inception_remote_backup_port', label: 'Backup port', input: 'number', placeholder: 'Backup database port' },
      { key: 'inception_remote_backup_user', label: 'Backup user', input: 'text', placeholder: 'Backup database user' },
      { key: 'inception_remote_backup_password', label: 'Backup password', input: 'password', placeholder: 'Backup database password' },
    ],
  },
  {
    id: 'sql-release',
    title: 'SQL Release Controls',
    description: 'Define auto-review behavior, review gates, and execution controls for SQL release workflows.',
    fields: [
      { key: 'critical_ddl_regex', label: 'Critical DDL regex', input: 'text', placeholder: 'Regex for statements that should be blocked' },
      { key: 'auto_review_wrong', label: 'Auto review reject level', input: 'number', placeholder: '1 rejects warnings, 2 rejects errors only' },
      { key: 'enable_backup_switch', label: 'Allow backup toggle', input: 'checkbox', description: 'When disabled, backup is always enforced.' },
      { key: 'auto_review', label: 'Enable auto review', input: 'checkbox', description: 'Allow automatic approval when all auto-review rules pass.' },
      { key: 'auto_review_tag', label: 'Auto review instance tags', input: 'multiselect', optionSource: 'instance_tags', showWhen: { key: 'auto_review', equals: true } },
      { key: 'auto_review_db_type', label: 'Auto review database types', input: 'multiselect', optionSource: 'auto_review_db_types', showWhen: { key: 'auto_review', equals: true } },
      { key: 'auto_review_regex', label: 'Auto review exclusion regex', input: 'text', placeholder: 'Statements matching this regex require manual review', showWhen: { key: 'auto_review', equals: true } },
      { key: 'auto_review_max_update_rows', label: 'Auto review max updated rows', input: 'number', placeholder: 'Maximum rows allowed for automatic approval', showWhen: { key: 'auto_review', equals: true } },
      { key: 'manual', label: 'Require manual execution confirmation', input: 'checkbox' },
      { key: 'ddl_dml_separation', label: 'Separate DDL and DML', input: 'checkbox', description: 'Reject MySQL submissions that mix DDL and DML.' },
      { key: 'ban_self_audit', label: 'Ban self approval', input: 'checkbox' },
      { key: 'real_row_count', label: 'Use actual affected row count', input: 'checkbox' },
    ],
  },
  {
    id: 'query-export',
    title: 'Query And Export Limits',
    description: 'Set query masking, timeouts, and export limits for online query and export flows.',
    fields: [
      { key: 'data_masking', label: 'Enable data masking', input: 'checkbox' },
      { key: 'query_check', label: 'Require masking validation', input: 'checkbox' },
      { key: 'disable_star', label: 'Disallow SELECT *', input: 'checkbox' },
      { key: 'max_execution_time', label: 'Max execution time (seconds)', input: 'number', placeholder: 'Default 60 seconds when unset' },
      { key: 'admin_query_limit', label: 'Admin query limit', input: 'number', placeholder: 'Row limit for admin and DBA query results' },
      { key: 'max_export_rows', label: 'Max export rows', input: 'number', placeholder: 'Default 10000 rows when unset' },
    ],
  },
  {
    id: 'background-jobs',
    title: 'Background Jobs',
    description: 'Choose the async execution backend and configure optional Celery scale-out settings.',
    fields: [
      { key: 'task_backend', label: 'Task backend', input: 'select', optionSource: 'task_backends', defaultValue: 'django_q' },
      {
        key: 'celery_broker_url',
        label: 'Celery broker URL',
        input: 'password',
        placeholder: 'redis://host:6379/1',
        showWhen: { key: 'task_backend', equals: 'celery' },
      },
      {
        key: 'celery_result_backend',
        label: 'Celery result backend',
        input: 'password',
        placeholder: 'redis://host:6379/2',
        showWhen: { key: 'task_backend', equals: 'celery' },
      },
      {
        key: 'celery_task_default_queue',
        label: 'Celery default queue',
        input: 'text',
        defaultValue: 'default',
        showWhen: { key: 'task_backend', equals: 'celery' },
      },
      {
        key: 'celery_task_soft_time_limit',
        label: 'Celery soft time limit (seconds)',
        input: 'number',
        showWhen: { key: 'task_backend', equals: 'celery' },
      },
      {
        key: 'celery_task_time_limit',
        label: 'Celery hard time limit (seconds)',
        input: 'number',
        showWhen: { key: 'task_backend', equals: 'celery' },
      },
    ],
  },
  {
    id: 'storage',
    title: 'Export Storage',
    description: 'Choose where generated export files are stored and test the active storage backend.',
    testAction: 'storage',
    fields: [
      { key: 'storage_type', label: 'Storage type', input: 'select', optionSource: 'storage_types', defaultValue: 'local' },
      { key: 'sftp_host', label: 'SFTP host', input: 'text', showWhen: { key: 'storage_type', equals: 'sftp' } },
      { key: 'sftp_port', label: 'SFTP port', input: 'number', showWhen: { key: 'storage_type', equals: 'sftp' } },
      { key: 'sftp_user', label: 'SFTP user', input: 'text', showWhen: { key: 'storage_type', equals: 'sftp' } },
      { key: 'sftp_password', label: 'SFTP password', input: 'password', showWhen: { key: 'storage_type', equals: 'sftp' } },
      { key: 'sftp_path', label: 'SFTP path', input: 'text', showWhen: { key: 'storage_type', equals: 'sftp' } },
      { key: 'sftp_custom_params', label: 'SFTP custom params', input: 'textarea', rows: 3, placeholder: '{"key":"value"}', showWhen: { key: 'storage_type', equals: 'sftp' } },
      { key: 's3c_access_key_id', label: 'S3 access key', input: 'password', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 's3c_access_key_secret', label: 'S3 secret key', input: 'password', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 's3c_endpoint', label: 'S3 endpoint', input: 'text', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 's3c_region', label: 'S3 region', input: 'text', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 's3c_bucket_name', label: 'S3 bucket', input: 'text', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 's3c_path', label: 'S3 path prefix', input: 'text', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 's3c_custom_params', label: 'S3 custom params', input: 'textarea', rows: 3, placeholder: '{"key":"value"}', showWhen: { key: 'storage_type', equals: 's3c' } },
      { key: 'azure_container', label: 'Azure container', input: 'text', showWhen: { key: 'storage_type', equals: 'azure' } },
      { key: 'azure_account_name', label: 'Azure account name', input: 'text', showWhen: { key: 'storage_type', equals: 'azure' } },
      { key: 'azure_account_key', label: 'Azure account key', input: 'password', showWhen: { key: 'storage_type', equals: 'azure' } },
      { key: 'azure_path', label: 'Azure path prefix', input: 'text', showWhen: { key: 'storage_type', equals: 'azure' } },
      { key: 'azure_custom_params', label: 'Azure custom params', input: 'textarea', rows: 3, placeholder: '{"key":"value"}', showWhen: { key: 'storage_type', equals: 'azure' } },
    ],
  },
  {
    id: 'notifications',
    title: 'Notifications',
    description: 'Control workflow notification targets, email delivery, chat integrations, and SMS providers.',
    testAction: 'email',
    fields: [
      { key: 'archery_base_url', label: 'Archery base URL', input: 'text', placeholder: 'https://archery.example.com' },
      { key: 'ddl_notify_auth_group', label: 'DDL notify groups', input: 'multiselect', optionSource: 'auth_groups' },
      { key: 'notify_phase_control', label: 'Notify phases', input: 'multiselect', optionSource: 'notify_phases', defaultValue: ['Apply', 'Pass', 'Execute', 'Cancel'] },
      { key: 'mail', label: 'Enable email notifications', input: 'checkbox' },
      { key: 'mail_ssl', label: 'Use SMTP SSL', input: 'checkbox', showWhen: { key: 'mail', equals: true } },
      { key: 'mail_smtp_server', label: 'SMTP server', input: 'text', showWhen: { key: 'mail', equals: true } },
      { key: 'mail_smtp_port', label: 'SMTP port', input: 'number', showWhen: { key: 'mail', equals: true } },
      { key: 'mail_smtp_user', label: 'SMTP user', input: 'text', showWhen: { key: 'mail', equals: true } },
      { key: 'mail_smtp_password', label: 'SMTP password', input: 'password', showWhen: { key: 'mail', equals: true } },
      { key: 'wx', label: 'Enable WeCom app notifications', input: 'checkbox' },
      { key: 'wx_corpid', label: 'WeCom CorpId', input: 'text', showWhen: { key: 'wx', equals: true } },
      { key: 'wx_agent_id', label: 'WeCom AgentId', input: 'text', showWhen: { key: 'wx', equals: true } },
      { key: 'wx_app_secret', label: 'WeCom app secret', input: 'password', showWhen: { key: 'wx', equals: true } },
      { key: 'qywx_webhook', label: 'Enable WeCom group bot', input: 'checkbox' },
      { key: 'feishu_webhook', label: 'Enable Feishu webhook', input: 'checkbox' },
      { key: 'feishu', label: 'Enable Feishu app notifications', input: 'checkbox' },
      { key: 'feishu_appid', label: 'Feishu app id', input: 'text', showWhen: { key: 'feishu', equals: true } },
      { key: 'feishu_app_secret', label: 'Feishu app secret', input: 'password', showWhen: { key: 'feishu', equals: true } },
      { key: 'generic_webhook_url', label: 'Generic webhook URL', input: 'text' },
      { key: 'sms_provider', label: 'SMS provider', input: 'select', optionSource: 'sms_providers', defaultValue: 'disabled' },
      { key: 'aliyun_access_key_id', label: 'Aliyun access key id', input: 'text', showWhen: { key: 'sms_provider', equals: 'aliyun' } },
      { key: 'aliyun_access_key_secret', label: 'Aliyun access key secret', input: 'password', showWhen: { key: 'sms_provider', equals: 'aliyun' } },
      { key: 'aliyun_sign_name', label: 'Aliyun sign name', input: 'text', showWhen: { key: 'sms_provider', equals: 'aliyun' } },
      { key: 'aliyun_template_code', label: 'Aliyun template code', input: 'text', showWhen: { key: 'sms_provider', equals: 'aliyun' } },
      { key: 'aliyun_variable_name', label: 'Aliyun variable name', input: 'text', showWhen: { key: 'sms_provider', equals: 'aliyun' } },
      { key: 'tencent_secret_id', label: 'Tencent secret id', input: 'text', showWhen: { key: 'sms_provider', equals: 'tencent' } },
      { key: 'tencent_secret_key', label: 'Tencent secret key', input: 'password', showWhen: { key: 'sms_provider', equals: 'tencent' } },
      { key: 'tencent_sign_name', label: 'Tencent sign name', input: 'text', showWhen: { key: 'sms_provider', equals: 'tencent' } },
      { key: 'tencent_template_id', label: 'Tencent template id', input: 'text', showWhen: { key: 'sms_provider', equals: 'tencent' } },
      { key: 'tencent_sdk_appid', label: 'Tencent SDK app id', input: 'text', showWhen: { key: 'sms_provider', equals: 'tencent' } },
    ],
  },
  {
    id: 'integrations',
    title: 'Integrations And AI',
    description: 'Maintain analyzer binaries, OpenAI defaults, and related integration endpoints used by Datamingle.',
    fields: [
      { key: 'sqladvisor', label: 'SQLAdvisor binary path', input: 'text' },
      { key: 'soar', label: 'SOAR binary path', input: 'text' },
      { key: 'soar_test_dsn', label: 'SOAR test DSN', input: 'text' },
      { key: 'gh_ost', label: 'gh-ost binary path', input: 'text' },
      { key: 'pt_osc', label: 'pt-online-schema-change binary path', input: 'text' },
      { key: 'openai_base_url', label: 'OpenAI base URL', input: 'text' },
      { key: 'openai_api_key', label: 'OpenAI API key', input: 'password' },
      { key: 'default_chat_model', label: 'Default chat model', input: 'text', defaultValue: 'gpt-3.5-turbo' },
      { key: 'default_query_template', label: 'Default query template', input: 'textarea', rows: 5, defaultValue: DEFAULT_QUERY_TEMPLATE },
      { key: 'my2sql', label: 'my2sql binary path', input: 'text' },
    ],
  },
  {
    id: 'login-security',
    title: 'Login, Access, And Defaults',
    description: 'Configure login defaults, first-login assignments, API user allowlists, and login-hardening thresholds.',
    fields: [
      { key: 'index_path_url', label: 'Default landing path', input: 'text', placeholder: 'sqlworkflow' },
      { key: 'default_auth_group', label: 'Default auth groups', input: 'multiselect', optionSource: 'auth_groups' },
      { key: 'default_resource_group', label: 'Default resource groups', input: 'multiselect', optionSource: 'resource_groups' },
      { key: 'api_user_whitelist', label: 'API user whitelist', input: 'multiselect', optionSource: 'users' },
      { key: 'lock_time_threshold', label: 'Account lock time (seconds)', input: 'number' },
      { key: 'lock_cnt_threshold', label: 'Failed login lock count', input: 'number' },
      { key: 'sign_up_enabled', label: 'Allow sign-up', input: 'checkbox' },
      { key: 'watermark_enabled', label: 'Enable watermark', input: 'checkbox' },
      { key: 'enforce_2fa', label: 'Enforce 2FA', input: 'checkbox' },
    ],
  },
  {
    id: 'branding',
    title: 'Announcements And Branding',
    description: 'Manage instance-wide announcements and small UI branding details for the Datamingle shell.',
    fields: [
      { key: 'announcement_content_enabled', label: 'Enable announcement banner', input: 'checkbox' },
      { key: 'announcement_content', label: 'Announcement content', input: 'textarea', rows: 4, showWhen: { key: 'announcement_content_enabled', equals: true } },
      { key: 'custom_title_suffix', label: 'Custom title suffix', input: 'text' },
    ],
  },
]

function cloneValue(value: SystemSettingsValue): SystemSettingsValue {
  if (Array.isArray(value)) {
    return [...value]
  }
  return value
}

function defaultValueForField(field: SystemSettingsFieldDefinition): SystemSettingsValue {
  if (field.defaultValue !== undefined) {
    return cloneValue(field.defaultValue)
  }
  if (field.input === 'checkbox') {
    return false
  }
  if (field.input === 'multiselect') {
    return []
  }
  if (field.input === 'number') {
    return null
  }
  return ''
}

export function createInitialSystemSettings(): SystemSettings {
  const initial: SystemSettings = {}

  for (const section of systemSettingsSections) {
    for (const field of section.fields) {
      if (!(field.key in initial)) {
        initial[field.key] = defaultValueForField(field)
      }
    }
  }

  return initial
}
