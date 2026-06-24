import os
import datetime

os.environ.setdefault(
    "FIELD_ENCRYPTION_KEYS", "9R_Jxat_be2SV-UbCS0dAYQ0SGjZVf0JyN-VPkVNyi0="
)

import pytest
from pytest_mock import MockFixture
from django.contrib.auth.models import Group

from common.utils.const import WorkflowStatus
from sql.models import (
    Instance,
    Team,
    SqlWorkflow,
    SqlWorkflowContent,
    QueryPrivilegesApply,
    ArchiveConfig,
    WorkflowAudit,
)
from common.config import SysConfig
from sql.utils.workflow_audit import AuditV2, AuditSetting


@pytest.fixture
def normal_user(django_user_model):
    user = django_user_model.objects.create(
        username="test_user", display="Chinese Display", is_active=True
    )
    yield user
    user.delete()


@pytest.fixture
def super_user(django_user_model):
    user = django_user_model.objects.create(
        username="super_user", display="Super User", is_active=True, is_superuser=True
    )
    yield user
    user.delete()


@pytest.fixture
def db_instance(db):
    ins = Instance.objects.create(
        instance_name="some_ins",
        type="slave",
        db_type="mysql",
        host="some_host",
        port=3306,
        user="ins_user",
        password="some_str",
    )
    yield ins
    ins.delete()


@pytest.fixture
def team(db) -> Team:
    res_group = Team.objects.create(team_id=1, team_name="team_name")
    yield res_group
    res_group.delete()


@pytest.fixture
def sql_workflow(db_instance):
    wf = SqlWorkflow.objects.create(
        workflow_name="some_name",
        team_id=1,
        team_name="g1",
        engineer_display="",
        audit_auth_groups="some_audit_group",
        create_time=datetime.datetime.now(),
        status="workflow_timingtask",
        is_backup=True,
        instance=db_instance,
        db_name="some_db",
        syntax_type=1,
    )
    wf_content = SqlWorkflowContent.objects.create(
        workflow=wf, sql_content="some_sql", execute_result=""
    )
    yield wf, wf_content
    wf.delete()
    wf_content.delete()


@pytest.fixture
def sql_query_apply(db_instance):
    tomorrow = datetime.datetime.today() + datetime.timedelta(days=1)
    query_apply_1 = QueryPrivilegesApply.objects.create(
        team_id=1,
        team_name="some_name",
        title="some_title1",
        user_name="some_user",
        instance=db_instance,
        db_list="some_db,some_db2",
        limit_num=100,
        valid_date=tomorrow,
        priv_type=1,
        status=0,
        audit_auth_groups="1",
    )
    yield query_apply_1
    query_apply_1.delete()


@pytest.fixture
def archive_apply(db_instance, team):
    archive_apply_1 = ArchiveConfig.objects.create(
        title="title",
        team=team,
        audit_auth_groups="",
        src_instance=db_instance,
        src_db_name="src_db_name",
        src_table_name="src_table_name",
        dest_instance=db_instance,
        dest_db_name="src_db_name",
        dest_table_name="src_table_name",
        condition="1=1",
        mode="file",
        no_delete=True,
        sleep=1,
        status=WorkflowStatus.WAITING,
        state=False,
        user_name="some_user",
        user_display="display",
    )
    yield archive_apply_1
    archive_apply_1.delete()


@pytest.fixture
def setup_sys_config(db):
    sys_config = SysConfig()
    yield sys_config
    sys_config.purge()


@pytest.fixture
def create_auth_group(db):
    auth_group = Group.objects.create(name="test_group")
    yield auth_group
    auth_group.delete()


@pytest.fixture
def fake_generate_audit_setting(mocker: MockFixture, super_user, create_auth_group):
    super_user.groups.add(create_auth_group)
    mock_generate_audit_setting = mocker.patch.object(AuditV2, "generate_audit_setting")
    fake_audit_setting = AuditSetting(
        auto_pass=False,
        audit_auth_groups=[create_auth_group.id],
    )
    mock_generate_audit_setting.return_value = fake_audit_setting
    yield mock_generate_audit_setting


@pytest.fixture
def create_team(db):
    team = Team.objects.create(
        team_name="team_name",
        is_deleted=False,
        qywx_webhook="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx",
        feishu_webhook="https://open.feishu.cn/open-apis/bot/v2/hook/xxx",
    )
    yield team
    team.delete()


@pytest.fixture
def create_audit_workflow(normal_user, create_team):
    audit_wf = WorkflowAudit.objects.create(
        team_id=create_team.team_id,
        team_name=create_team.team_name,
        workflow_id=1,
        workflow_type=2,
        workflow_title="Apply Title",
        workflow_remark="Apply Remark",
        audit_auth_groups="1",
        current_audit="1",
        next_audit="2",
        current_status=0,
        create_user=normal_user.username,
    )
    yield audit_wf
    audit_wf.delete()


@pytest.fixture
def clean_auth_group(db):
    yield
    Group.objects.all().delete()
