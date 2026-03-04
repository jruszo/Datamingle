# -*- coding: UTF-8 -*-
import dataclasses
import importlib
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Union, Optional, List
import logging

from django.contrib.auth.models import Group
from django.utils import timezone
from django.conf import settings

from sql.engines.models import ReviewResult
from sql.utils.resource_group import user_groups, auth_group_users
from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from sql.models import (
    WorkflowAudit,
    WorkflowAuditDetail,
    WorkflowAuditSetting,
    WorkflowLog,
    ResourceGroup,
    SqlWorkflow,
    QueryPrivilegesApply,
    Users,
    ArchiveConfig,
)
from common.config import SysConfig
from sql.utils.sql_utils import remove_comments

logger = logging.getLogger("default")


class AuditException(Exception):
    pass


class ReviewNodeType(Enum):
    GROUP = "group"
    AUTO_PASS = "auto_pass"


@dataclass
class ReviewNode:
    group: Optional[Group] = None
    node_type: ReviewNodeType = ReviewNodeType.GROUP
    is_current_node: bool = False
    is_passed_node: bool = False

    def __post_init__(self):
        if self.node_type == ReviewNodeType.GROUP and not self.group:
            raise ValueError(
                f"group not provided and node_type is set as {self.node_type}"
            )

    @property
    def is_auto_pass(self):
        return self.node_type == ReviewNodeType.AUTO_PASS


@dataclass
class ReviewInfo:
    nodes: List[ReviewNode] = field(default_factory=list)
    current_node_index: int = None

    @property
    def readable_info(self) -> str:
        """Generate a readable workflow, e.g. g1(passed) -> g2(current) -> g3.
        Primarily used for rendering messages.
        """
        steps = []
        for index, n in enumerate(self.nodes):
            if n.is_current_node:
                self.current_node_index = index
                steps.append(f"{n.group.name}(current)")
                continue
            if n.is_passed_node:
                steps.append(f"{n.group.name}(passed)")
                continue
            steps.append(n.group.name)
        return " -> ".join(steps)

    @property
    def current_node(self) -> ReviewNode:
        if self.current_node_index:
            return self.nodes[self.current_node_index]
        for index, n in enumerate(self.nodes):
            if n.is_current_node:
                self.current_node_index = n
                return n


@dataclass
class AuditSetting:
    """
    `audit_auth_groups` are Django group IDs.
    """

    audit_auth_groups: List = field(default_factory=list)
    auto_pass: bool = False
    auto_reject: bool = False

    @property
    def audit_auth_group_in_db(self):
        if self.auto_reject or self.auto_pass:
            return ""
        return ",".join(str(x) for x in self.audit_auth_groups)


# List allowed operations for each workflow status during review.
SUPPORTED_OPERATION_GRID = {
    WorkflowStatus.WAITING.value: [
        WorkflowAction.PASS,
        WorkflowAction.REJECT,
        WorkflowAction.ABORT,
    ],
    WorkflowStatus.PASSED.value: [
        WorkflowAction.REJECT,
        WorkflowAction.ABORT,
        WorkflowAction.EXECUTE_SET_TIME,
        WorkflowAction.EXECUTE_START,
        WorkflowAction.EXECUTE_END,
    ],
    WorkflowStatus.REJECTED.value: [],
    WorkflowStatus.ABORTED.value: [],
}


@dataclass
class AuditV2:
    # The workflow object may not have been created in DB yet.
    workflow: Union[SqlWorkflow, ArchiveConfig, QueryPrivilegesApply] = None
    sys_config: SysConfig = field(default_factory=SysConfig)
    audit: WorkflowAudit = None
    workflow_type: WorkflowType = WorkflowType.SQL_REVIEW
    # ArchiveConfig does not contain these two fields, so they are required.
    resource_group: str = ""
    resource_group_id: int = 0

    def __post_init__(self):
        if not self.workflow:
            if not self.audit:
                raise ValueError("WorkflowAudit or workflow is required")
            self.get_workflow()
        self.workflow_type = self.workflow.workflow_type
        if isinstance(self.workflow, SqlWorkflow):
            self.resource_group = self.workflow.group_name
            self.resource_group_id = self.workflow.group_id
        elif isinstance(self.workflow, ArchiveConfig):
            try:
                group_in_db = ResourceGroup.objects.get(group_name=self.resource_group)
                self.resource_group_id = group_in_db.group_id
            except ResourceGroup.DoesNotExist:
                raise AuditException(
                    f"Invalid parameter: resource group {self.resource_group} not found"
                )
        elif isinstance(self.workflow, QueryPrivilegesApply):
            self.resource_group = self.workflow.group_name
            self.resource_group_id = self.workflow.group_id
        # This may fail to get an approval flow for new workflows; do not raise.
        self.get_audit_info()
        # Prevent explicit `None` passed from `get_auditor`.
        if not self.sys_config:
            self.sys_config = SysConfig()

    @property
    def review_info(self) -> (str, str):
        """Get readable approval flow info, including the whole flow and current node."""
        if self.audit.audit_auth_groups == "":
            audit_auth_group = "No approval required"
        else:
            try:
                audit_auth_group = "->".join(
                    [
                        Group.objects.get(id=auth_group_id).name
                        for auth_group_id in self.audit.audit_auth_groups.split(",")
                    ]
                )
            except Group.DoesNotExist:
                audit_auth_group = self.audit.audit_auth_groups
        if self.audit.current_audit == "-1":
            current_audit_auth_group = None
        else:
            try:
                current_audit_auth_group = Group.objects.get(
                    id=self.audit.current_audit
                ).name
            except Group.DoesNotExist:
                current_audit_auth_group = self.audit.current_audit
        return audit_auth_group, current_audit_auth_group

    def get_workflow(self):
        """Try to get workflow from audit."""
        self.workflow = self.audit.get_workflow()
        if self.audit.workflow_type == WorkflowType.ARCHIVE:
            self.resource_group = self.audit.group_name
            self.resource_group_id = self.audit.group_id

    def is_auto_reject(self):
        """Whether system should auto-reject this workflow."""
        if self.workflow_type != WorkflowType.SQL_REVIEW:
            return False
        # Decide reject/pass behavior from system config.
        auto_review_wrong = self.sys_config.get(
            "auto_review_wrong", ""
        )  # 1: reject on warning; 2/empty: reject only on error.
        review_content = self.workflow.sqlworkflowcontent.review_content or "[]"
        warning_count, error_count = 0, 0
        for r in json.loads(review_content):
            err_level = ReviewResult(**r).errlevel
            if err_level == 1:
                warning_count += 1
            if err_level == 2:
                error_count += 1
        if any(
            [
                warning_count > 0 and auto_review_wrong == "1",
                error_count > 0 and auto_review_wrong in ("", "1", "2"),
            ]
        ):
            return True
        return False

    def is_auto_review(self) -> bool:
        if self.is_auto_reject():
            return False
        if self.workflow_type != WorkflowType.SQL_REVIEW:
            # Auto review currently only applies to SQL review workflows.
            return False
        if self.workflow.is_offline_export:
            # Do not auto-review export workflows.
            return False
        auto_review_enabled = self.sys_config.get("auto_review", False)
        if not auto_review_enabled:
            return False
        auto_review_tags = self.sys_config.get("auto_review_tag", "").split(",")
        auto_review_db_type = self.sys_config.get("auto_review_db_type", "").split(",")
        # TODO: This can be moved to engine, but config may become complex.
        if self.workflow.instance.db_type not in auto_review_db_type:
            return False
        if not self.workflow.instance.instance_tag.filter(
            tag_code__in=auto_review_tags
        ).exists():
            return False

        # Load regex pattern.
        auto_review_regex = self.sys_config.get(
            "auto_review_regex",
            "^alter|^create|^drop|^truncate|^rename|^delete|^del|^flushdb|^flushall|^lpop|^rpop",
        )
        p = re.compile(auto_review_regex, re.I)

        # Check whether any statement requires manual review.
        all_affected_rows = 0
        review_content = self.workflow.sqlworkflowcontent.review_content
        for review_row in json.loads(review_content):
            review_result = ReviewResult(**review_row)
            # Remove SQL comments https://github.com/hhyo/Archery/issues/949
            sql = remove_comments(review_result.sql).replace("\n", "").replace("\r", "")
            # Regex match.
            if p.match(sql):
                # Matched means manual review is required.
                return False
            # Check affected rows, manual review is needed over the threshold.
            all_affected_rows += int(review_result.affected_rows)
        if all_affected_rows > int(
            self.sys_config.get("auto_review_max_update_rows", 50)
        ):
            # Affected rows exceed threshold, manual review is required.
            return False
        return True

    def generate_audit_setting(self) -> AuditSetting:
        if self.workflow_type in [WorkflowType.SQL_REVIEW, WorkflowType.QUERY]:
            group_id = self.workflow.group_id
        else:
            # ArchiveConfig
            group_id = self.resource_group_id
        try:
            workflow_audit_setting = WorkflowAuditSetting.objects.get(
                workflow_type=self.workflow_type, group_id=group_id
            )
        except WorkflowAuditSetting.DoesNotExist:
            raise AuditException(
                f"Approval flow is not configured for workflow type {self.workflow_type.label}"
            )
        return AuditSetting(
            auto_pass=self.is_auto_review(),
            auto_reject=self.is_auto_reject(),
            audit_auth_groups=workflow_audit_setting.audit_auth_groups.split(","),
        )

    def create_audit(self) -> str:
        """Create audit flow for the given workflow.
        Returns a message. Raises exception on error.
        """
        # Check if there is already a pending audit.
        workflow_info = self.get_audit_info()
        if workflow_info:
            raise AuditException(
                "This workflow is currently pending approval, do not submit repeatedly"
            )
        # Get audit setting.
        audit_setting = self.generate_audit_setting()

        if self.workflow_type == WorkflowType.QUERY:
            workflow_title = self.workflow.title
            group_id = self.workflow.group_id
            group_name = self.workflow.group_name
            create_user = self.workflow.user_name
            create_user_display = self.workflow.user_display
            self.workflow.audit_auth_groups = audit_setting.audit_auth_group_in_db
        elif self.workflow_type == WorkflowType.SQL_REVIEW:
            workflow_title = self.workflow.workflow_name
            group_id = self.workflow.group_id
            group_name = self.workflow.group_name
            create_user = self.workflow.engineer
            create_user_display = self.workflow.engineer_display
            self.workflow.audit_auth_groups = audit_setting.audit_auth_group_in_db
        elif self.workflow_type == WorkflowType.ARCHIVE:
            workflow_title = self.workflow.title
            group_id = self.resource_group_id
            group_name = self.resource_group
            create_user = self.workflow.user_name
            create_user_display = self.workflow.user_display
            self.workflow.audit_auth_groups = audit_setting.audit_auth_group_in_db
        else:
            raise AuditException(
                f"Unsupported workflow type: {self.workflow_type.label}"
            )
        self.workflow.save()
        self.audit = WorkflowAudit(
            group_id=group_id,
            group_name=group_name,
            workflow_id=self.workflow.pk,
            workflow_type=self.workflow_type,
            workflow_title=workflow_title,
            audit_auth_groups=audit_setting.audit_auth_group_in_db,
            current_audit="-1",
            next_audit="-1",
            create_user=create_user,
            create_user_display=create_user_display,
        )
        # Auto-review branch.
        if audit_setting.auto_reject:
            self.audit.current_status = WorkflowStatus.REJECTED
            self.audit.save()
            WorkflowLog.objects.create(
                audit_id=self.audit.audit_id,
                operation_type=WorkflowAction.SUBMIT,
                operation_type_desc=WorkflowAction.SUBMIT.label,
                operation_info="System auto-rejected",
                operator=self.audit.create_user,
                operator_display=self.audit.create_user_display,
            )

            return "Auto-rejected"
        elif audit_setting.auto_pass:
            self.audit.current_status = WorkflowStatus.PASSED
            self.audit.save()
            WorkflowLog.objects.create(
                audit_id=self.audit.audit_id,
                operation_type=WorkflowAction.SUBMIT,
                operation_type_desc=WorkflowAction.SUBMIT.label,
                operation_info="No approval required, system auto-approved",
                operator=self.audit.create_user,
                operator_display=self.audit.create_user_display,
            )

            return "No approval required, auto-approved"

        # Insert pending review data to audit main table.
        self.audit.current_audit = audit_setting.audit_auth_groups[0]
        # Determine whether there is next review node.
        if len(audit_setting.audit_auth_groups) == 1:
            self.audit.next_audit = "-1"
        else:
            self.audit.next_audit = audit_setting.audit_auth_groups[1]

        self.audit.current_status = WorkflowStatus.WAITING
        self.audit.create_user = create_user
        self.audit.create_user_display = create_user_display
        self.audit.save()
        readable_review_flow, _ = self.review_info
        audit_log = WorkflowLog(
            audit_id=self.audit.audit_id,
            operation_type=WorkflowAction.SUBMIT,
            operation_type_desc=WorkflowAction.SUBMIT.label,
            operation_info="Waiting for approval, flow: {}".format(
                readable_review_flow
            ),
            operator=self.audit.create_user,
            operator_display=self.audit.create_user_display,
        )
        audit_log.save()
        return "Workflow submitted successfully"

    def can_operate(self, action: WorkflowAction, actor: Users):
        """Check whether user has permission to operate this workflow."""
        # First check workflow status and operation compatibility.
        allowed_actions = SUPPORTED_OPERATION_GRID.get(self.audit.current_status)
        if not allowed_actions:
            raise AuditException(
                f"Operation not allowed: current workflow status is "
                f"{self.audit.current_status}, no actions are allowed"
            )
        if action not in allowed_actions:
            raise AuditException(
                f"Operation not allowed: current workflow status is "
                f"{self.audit.current_status}, allowed actions are "
                f"{','.join(x.label for x in allowed_actions)}"
            )
        if self.workflow_type == WorkflowType.QUERY:
            need_user_permission = "sql.query_review"
        elif self.workflow_type == WorkflowType.SQL_REVIEW:
            need_user_permission = "sql.sql_review"
        elif self.workflow_type == WorkflowType.ARCHIVE:
            need_user_permission = "sql.archive_review"
        else:
            raise AuditException(f"Unsupported workflow type: {self.workflow_type}")

        if action == WorkflowAction.ABORT:
            if actor.username != self.audit.create_user:
                raise AuditException("Only the workflow submitter can abort it")
            return True
        if action in [WorkflowAction.PASS, WorkflowAction.REJECT]:
            # Permission checks are required.
            # Superuser can review all workflows.
            if actor.is_superuser:
                return True
            # Check self-review.
            if actor.username == self.audit.create_user and self.sys_config.get(
                "ban_self_audit"
            ):
                raise AuditException(
                    "Current configuration forbids reviewing your own workflow"
                )
            # Check user permission.
            if not actor.has_perm(need_user_permission):
                raise AuditException(
                    "User has no related review permission, please configure permissions"
                )

            # Check whether user is in the current review group.
            try:
                audit_auth_group = Group.objects.get(id=self.audit.current_audit)
            except Group.DoesNotExist:
                raise AuditException(
                    "Current review permission group does not exist, "
                    "please ask admin to check and clean bad data"
                )
            if not auth_group_users([audit_auth_group.name], self.resource_group_id):
                raise AuditException(
                    "User is not in the resource group for this flow, no permission"
                )
            if not actor.groups.filter(id=self.audit.current_audit).exists():
                raise AuditException(
                    "User is not in the current node review group, no permission"
                )
            return True
        if action in [
            WorkflowAction.EXECUTE_START,
            WorkflowAction.EXECUTE_END,
            WorkflowAction.EXECUTE_SET_TIME,
        ]:
            # Usually this is a system-driven transition.
            return True

        raise AuditException("Unsupported operation, cannot determine permission")

    def operate(
        self, action: WorkflowAction, actor: Users, remark: str
    ) -> WorkflowAuditDetail:
        """Operate on a submitted workflow."""
        if not self.audit:
            raise AuditException(
                "No audit info bound to the given workflow, cannot operate"
            )
        self.can_operate(action, actor)

        if action == WorkflowAction.PASS:
            return self.operate_pass(actor, remark)
        if action == WorkflowAction.REJECT:
            return self.operate_reject(actor, remark)
        if action == WorkflowAction.ABORT:
            return self.operate_abort(actor, remark)

    def get_audit_info(self) -> Optional[WorkflowAudit]:
        """Try to get audit workflow from current workflow."""
        if self.audit:
            return self.audit
        self.audit = self.workflow.get_audit()
        return self.audit

    def operate_pass(self, actor: Users, remark: str) -> WorkflowAuditDetail:
        # Check whether there is a next review level.
        if self.audit.next_audit == "-1":
            # No next level, mark as approved.
            self.audit.current_audit = "-1"
            self.audit.current_status = WorkflowStatus.PASSED
            self.audit.save()
        else:
            # Update current and next review groups in main audit record.
            self.audit.current_status = WorkflowStatus.WAITING
            self.audit.current_audit = self.audit.next_audit
            # Check whether there is another level after next.
            audit_auth_groups_list = self.audit.audit_auth_groups.split(",")
            try:
                position = audit_auth_groups_list.index(str(self.audit.current_audit))
            except ValueError as e:
                logger.error(
                    "Approval flow config error, current review node "
                    f"{self.audit.current_audit} is not in the flow: "
                    f"audit_id {self.audit.audit_id}"
                )
                raise e
            if position + 1 >= len(audit_auth_groups_list):
                # Last node.
                self.audit.next_audit = "-1"
            else:
                self.audit.next_audit = audit_auth_groups_list[position + 1]
            self.audit.save()

        # Insert audit detail row.
        audit_detail_result = WorkflowAuditDetail.objects.create(
            audit_id=self.audit.audit_id,
            audit_user=actor.username,
            audit_status=WorkflowStatus.PASSED,
            audit_time=timezone.now(),
            remark=remark,
        )

        if self.audit.current_audit == "-1":
            operation_info = f"Approval remark: {remark}, no next approval"
        else:
            try:
                next_group_name = Group.objects.get(id=self.audit.current_audit).name
            except Group.DoesNotExist:
                next_group_name = self.audit.current_audit
            operation_info = (
                f"Approval remark: {remark}, next approval: {next_group_name}"
            )

        # Add workflow log.
        WorkflowLog.objects.create(
            audit_id=self.audit.audit_id,
            operation_type=WorkflowAction.PASS,
            operation_type_desc=WorkflowAction.PASS.label,
            operation_info=operation_info,
            operator=actor.username,
            operator_display=actor.display,
        )
        return audit_detail_result

    def operate_reject(self, actor: Users, remark: str) -> WorkflowAuditDetail:
        # Update audit status in main table.
        self.audit.current_audit = "-1"
        self.audit.next_audit = "-1"
        self.audit.current_status = WorkflowStatus.REJECTED
        self.audit.save()
        # Insert audit detail row.
        workflow_audit_detail = WorkflowAuditDetail.objects.create(
            audit_id=self.audit.audit_id,
            audit_user=actor.username,
            audit_status=WorkflowStatus.REJECTED,
            audit_time=timezone.now(),
            remark=remark,
        )
        # Add workflow log.
        WorkflowLog.objects.create(
            audit_id=self.audit.audit_id,
            operation_type=2,
            operation_type_desc="Approval rejected",
            operation_info="Approval remark: {}".format(remark),
            operator=actor.username,
            operator_display=actor.display,
        )

        return workflow_audit_detail

    def operate_abort(self, actor: Users, remark: str) -> WorkflowAuditDetail:
        # Update audit status in main table.

        self.audit.next_audit = "-1"
        self.audit.current_status = WorkflowStatus.ABORTED
        self.audit.save()

        # Insert audit detail row.
        workflow_audit_detail = WorkflowAuditDetail.objects.create(
            audit_id=self.audit.audit_id,
            audit_user=actor.username,
            audit_status=WorkflowStatus.ABORTED,
            audit_time=timezone.now(),
            remark=remark,
        )
        # Add workflow log.
        WorkflowLog.objects.create(
            audit_id=self.audit.audit_id,
            operation_type=3,
            operation_type_desc="Approval aborted",
            operation_info="Abort reason: {}".format(remark),
            operator=actor.username,
            operator_display=actor.display,
        )
        return workflow_audit_detail

    def get_review_info(self) -> ReviewInfo:
        """Provide status for each node in the approval flow.
        If overall status is WAITING: nodes before current are passed, current
        node is current and unpassed, nodes after current are unpassed.
        For other overall statuses, node flags keep default values.
        """
        self.get_audit_info()
        review_nodes = []
        has_met_current_node = False
        current_node_group_id = int(self.audit.current_audit)
        for g in self.audit.audit_auth_groups.split(","):
            if not g:
                # Empty value means auto-pass.
                review_nodes.append(
                    ReviewNode(
                        node_type=ReviewNodeType.AUTO_PASS,
                        is_passed_node=True,
                    )
                )
                continue
            try:
                g = int(g)
            except ValueError:  # pragma: no cover
                # Dirty data, treat as auto-pass.
                # Compatibility: usually empty value means auto-pass.
                review_nodes.append(
                    ReviewNode(
                        node_type=ReviewNodeType.AUTO_PASS,
                        is_passed_node=True,
                    )
                )
                continue
            group_in_db = Group.objects.get(id=g)
            if self.audit.current_status != WorkflowStatus.WAITING:
                # Overall status is not waiting, do not set detailed flags.
                review_nodes.append(
                    ReviewNode(
                        group=group_in_db,
                    )
                )
                continue
            if current_node_group_id == g:
                # Current node is always unpassed.
                has_met_current_node = True
                review_nodes.append(
                    ReviewNode(
                        group=group_in_db,
                        is_current_node=True,
                        is_passed_node=False,
                    )
                )
                continue
            if has_met_current_node:
                # Nodes after current are always unpassed.
                review_nodes.append(
                    ReviewNode(
                        group=group_in_db,
                        is_passed_node=False,
                    )
                )
                continue
            # Otherwise, node is passed.
            review_nodes.append(
                ReviewNode(
                    group=group_in_db,
                    is_passed_node=True,
                )
            )
        return ReviewInfo(nodes=review_nodes)


class Audit(object):
    """Legacy Audit, prefer AuditV2 for new changes."""

    # Get pending workflow count for a user.
    @staticmethod
    def todo(user):
        # Get resource groups for the user.
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        # Get permission groups for the user.
        if user.is_superuser:
            auth_group_ids = [group.id for group in Group.objects.all()]
        else:
            auth_group_ids = [group.id for group in Group.objects.filter(user=user)]

        return WorkflowAudit.objects.filter(
            current_status=WorkflowStatus.WAITING,
            group_id__in=group_ids,
            current_audit__in=auth_group_ids,
        ).count()

    # Get audit info by audit_id.
    @staticmethod
    def detail(audit_id):
        try:
            return WorkflowAudit.objects.get(audit_id=audit_id)
        except Exception:
            return None

    # Get audit info by workflow_id.
    @staticmethod
    def detail_by_workflow_id(workflow_id, workflow_type) -> WorkflowAudit:
        try:
            return WorkflowAudit.objects.get(
                workflow_id=workflow_id, workflow_type=workflow_type
            )
        except Exception:
            return None

    # Get audit settings by group and workflow type.
    @staticmethod
    def settings(group_id, workflow_type):
        try:
            return WorkflowAuditSetting.objects.get(
                workflow_type=workflow_type, group_id=group_id
            ).audit_auth_groups
        except Exception:
            return None

    # Update or create settings.
    @staticmethod
    def change_settings(group_id, workflow_type, audit_auth_groups):
        try:
            WorkflowAuditSetting.objects.get(
                workflow_type=workflow_type, group_id=group_id
            )
            WorkflowAuditSetting.objects.filter(
                workflow_type=workflow_type, group_id=group_id
            ).update(audit_auth_groups=audit_auth_groups)
        except Exception:
            inset = WorkflowAuditSetting()
            inset.group_id = group_id
            inset.group_name = ResourceGroup.objects.get(group_id=group_id).group_name
            inset.audit_auth_groups = audit_auth_groups
            inset.workflow_type = workflow_type
            inset.save()

    # Determine whether user can review now.
    @staticmethod
    def can_review(user, workflow_id, workflow_type):
        audit_info = WorkflowAudit.objects.get(
            workflow_id=workflow_id, workflow_type=workflow_type
        )
        group_id = audit_info.group_id
        result = False

        def get_workflow_applicant(workflow_id, workflow_type):
            user = ""
            if workflow_type == 1:
                workflow = QueryPrivilegesApply.objects.get(apply_id=workflow_id)
                user = workflow.user_name
            elif workflow_type == 2:
                workflow = SqlWorkflow.objects.get(id=workflow_id)
                user = workflow.engineer
            elif workflow_type == 3:
                workflow = ArchiveConfig.objects.get(id=workflow_id)
                user = workflow.user_name
            return user

        applicant = get_workflow_applicant(workflow_id, workflow_type)
        if (
            user.username == applicant
            and not user.is_superuser
            and SysConfig().get("ban_self_audit")
        ):
            return result
        # Only workflows in waiting status are reviewable.
        if audit_info.current_status == WorkflowStatus.WAITING:
            try:
                auth_group_id = Audit.detail_by_workflow_id(
                    workflow_id, workflow_type
                ).current_audit
                audit_auth_group = Group.objects.get(id=auth_group_id).name
            except Exception:
                raise Exception(
                    "Current review auth_group_id does not exist, "
                    "please check and clean historical data"
                )
            if (
                user.is_superuser
                or auth_group_users([audit_auth_group], group_id)
                .filter(id=user.id)
                .exists()
            ):
                if workflow_type == 1:
                    if user.has_perm("sql.query_review"):
                        result = True
                elif workflow_type == 2:
                    if user.has_perm("sql.sql_review"):
                        result = True
                elif workflow_type == 3:
                    if user.has_perm("sql.archive_review"):
                        result = True
        return result

    # Add workflow log.
    @staticmethod
    def add_log(
        audit_id,
        operation_type,
        operation_type_desc,
        operation_info,
        operator,
        operator_display,
    ):
        log = WorkflowLog(
            audit_id=audit_id,
            operation_type=operation_type,
            operation_type_desc=operation_type_desc,
            operation_info=operation_info,
            operator=operator,
            operator_display=operator_display,
        )
        log.save()
        return log

    # Get workflow logs.
    @staticmethod
    def logs(audit_id):
        return WorkflowLog.objects.filter(audit_id=audit_id)


def get_auditor(
    # The workflow object may not have been created in DB yet.
    workflow: Union[SqlWorkflow, ArchiveConfig, QueryPrivilegesApply] = None,
    sys_config: SysConfig = None,
    audit: WorkflowAudit = None,
    workflow_type: WorkflowType = WorkflowType.SQL_REVIEW,
    # ArchiveConfig does not contain these two fields, so they are required.
    resource_group: str = "",
    resource_group_id: int = 0,
) -> AuditV2:
    current_auditor = settings.CURRENT_AUDITOR
    module, o = current_auditor.split(":")
    auditor = getattr(importlib.import_module(module), o)
    return auditor(
        workflow=workflow,
        workflow_type=workflow_type,
        sys_config=sys_config,
        audit=audit,
        resource_group=resource_group,
        resource_group_id=resource_group_id,
    )
