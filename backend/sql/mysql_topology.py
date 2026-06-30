import re
from collections.abc import Mapping

from django.db import IntegrityError, transaction
from django.utils import timezone

from common.config import SysConfig
from sql.models import Config, Instance, MysqlCluster, MysqlTopologyAlert

MYSQL_TOPOLOGY_RECONCILE_LOCK_NAME = "mysql_topology_reconcile_lock"
DRIFT_NOTIFY_BLOCK = "notify_block"
DRIFT_AUTO_DETACH = "auto_detach"
DRIFT_NOTIFY_ONLY = "notify_only"
DRIFT_POLICY_CHOICES = (
    DRIFT_NOTIFY_BLOCK,
    DRIFT_AUTO_DETACH,
    DRIFT_NOTIFY_ONLY,
)


def get_mysql_topology_drift_policy():
    policy = SysConfig().get("mysql_topology_drift_policy", DRIFT_NOTIFY_BLOCK)
    if policy not in DRIFT_POLICY_CHOICES:
        return DRIFT_NOTIFY_BLOCK
    return policy


def mysql_workflow_block_reason(instance):
    if instance.db_type != "mysql":
        return ""
    if instance.mysql_ddl_dml_eligible:
        return ""
    return (
        instance.mysql_ddl_dml_block_reason
        or "DDL/DML is not allowed for this MySQL topology target."
    )


def mysql_workflow_target_filter(queryset):
    return queryset.exclude(db_type="mysql", mysql_ddl_dml_eligible=False)


def apply_mysql_topology_snapshot(instance, snapshot, now=None):
    if instance.db_type != "mysql":
        return instance

    now = now or timezone.now()
    normalized = normalize_mysql_topology_snapshot(snapshot)
    with transaction.atomic():
        _lock_mysql_topology_reconciliation()
        current_instance = Instance.objects.select_for_update().get(pk=instance.pk)
        if (
            current_instance.mysql_topology_last_seen_at
            and current_instance.mysql_topology_last_seen_at > now
        ):
            instance.refresh_from_db()
            return instance
        instance.mysql_server_uuid = normalized["server_uuid"]
        instance.mysql_read_only = normalized["read_only"]
        instance.mysql_super_read_only = normalized["super_read_only"]
        instance.mysql_source_host = normalized["source_host"]
        instance.mysql_source_port = normalized["source_port"]
        instance.mysql_topology_details = normalized["details"]
        instance.mysql_topology_last_seen_at = now
        instance.save(
            update_fields=[
                "mysql_server_uuid",
                "mysql_read_only",
                "mysql_super_read_only",
                "mysql_source_host",
                "mysql_source_port",
                "mysql_topology_details",
                "mysql_topology_last_seen_at",
                "update_time",
            ]
        )
        _reconcile_mysql_topology(now=now)
    instance.refresh_from_db()
    return instance


def normalize_mysql_topology_snapshot(snapshot):
    payload = snapshot if isinstance(snapshot, Mapping) else {}
    source_host = (
        payload.get("source_host")
        or payload.get("master_host")
        or payload.get("primary_host")
        or ""
    )
    source_host = str(source_host or "").strip()
    source_port = (
        payload.get("source_port")
        or payload.get("master_port")
        or payload.get("primary_port")
    )
    if source_host and not source_port:
        source_port = 3306
    return {
        "server_uuid": str(payload.get("server_uuid") or "").strip(),
        "read_only": _nullable_bool(payload.get("read_only")),
        "super_read_only": _nullable_bool(payload.get("super_read_only")),
        "source_host": source_host,
        "source_port": _nullable_int(source_port),
        "details": dict(payload),
    }


def reconcile_mysql_topology(now=None):
    with transaction.atomic():
        _lock_mysql_topology_reconciliation()
        _reconcile_mysql_topology(now=now)


def _lock_mysql_topology_reconciliation():
    Config.objects.update_or_create(
        item=MYSQL_TOPOLOGY_RECONCILE_LOCK_NAME,
        defaults={
            "value": "1",
            "description": "Internal lock for MySQL topology reconciliation.",
        },
    )
    Config.objects.select_for_update().get(item=MYSQL_TOPOLOGY_RECONCILE_LOCK_NAME)


def _reconcile_mysql_topology(now=None):
    now = now or timezone.now()
    instances = list(
        Instance.objects.select_related("mysql_cluster")
        .filter(db_type="mysql", mysql_topology_last_seen_at__isnull=False)
        .order_by("id")
    )

    components = _mysql_components(instances)
    handled_instance_ids = set()
    touched_cluster_ids = set()
    for component in components:
        known = sorted(component["instances"], key=lambda item: item.id)
        if _is_standalone_component(component):
            for instance in known:
                _apply_standalone_component(instance, now=now)
            handled_instance_ids.update(instance.id for instance in known)
            continue
        cluster = _upsert_cluster_for_component(component, now=now)
        touched_cluster_ids.add(cluster.id)
        _apply_cluster_component(cluster, known, now=now)
        handled_instance_ids.update(instance.id for instance in known)

    for instance in instances:
        if instance.id not in handled_instance_ids:
            _apply_standalone_component(instance, now=now)
    _expire_stale_auto_clusters(touched_cluster_ids, now=now)


def _mysql_components(instances):
    endpoint_by_instance = {
        instance.id: _endpoint(instance.host, instance.port) for instance in instances
    }
    known_by_endpoint = {}
    for instance in instances:
        endpoint = endpoint_by_instance[instance.id]
        if endpoint:
            known_by_endpoint.setdefault(endpoint, []).append(instance)
    dsu = _DisjointSet()
    for endpoint in endpoint_by_instance.values():
        dsu.add(endpoint)
    for instance in instances:
        source_endpoint = _endpoint(
            instance.mysql_source_host, instance.mysql_source_port
        )
        if source_endpoint:
            dsu.union(endpoint_by_instance[instance.id], source_endpoint)
        for member_endpoint in _group_member_endpoints(instance):
            dsu.union(endpoint_by_instance[instance.id], member_endpoint)

    components = {}
    for endpoint in dsu.items:
        component = _component_for_endpoint(components, dsu, endpoint)
        component["endpoints"].add(endpoint)
        component["instances"].extend(known_by_endpoint.get(endpoint, []))
    for instance in instances:
        source_endpoint = _endpoint(
            instance.mysql_source_host, instance.mysql_source_port
        )
        if source_endpoint:
            component = _component_for_endpoint(components, dsu, source_endpoint)
            component["source_endpoints"].add(source_endpoint)
        primary_endpoint = _group_primary_endpoint(instance)
        if primary_endpoint:
            component = _component_for_endpoint(components, dsu, primary_endpoint)
            component["source_endpoints"].add(primary_endpoint)

    return [component for component in components.values() if component["instances"]]


def _component_for_endpoint(components, dsu, endpoint):
    root = dsu.find(endpoint)
    component = components.setdefault(
        root, {"endpoints": set(), "instances": [], "source_endpoints": set()}
    )
    component["endpoints"].add(endpoint)
    return component


def _is_standalone_component(component):
    return len(component["endpoints"]) == 1 and not component["source_endpoints"]


def _apply_standalone_component(instance, now):
    if instance.mysql_cluster_membership_source == MysqlCluster.SOURCE_MANUAL:
        if get_mysql_topology_drift_policy() == DRIFT_AUTO_DETACH:
            instance.mysql_cluster = None
            instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_AUTO
        else:
            _mark_instance_drift(instance, instance.mysql_cluster, now=now)
            return

    writable = _is_writable(instance)
    instance.mysql_cluster = None
    instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_AUTO
    instance.mysql_topology_role = Instance.MYSQL_ROLE_STANDALONE
    instance.mysql_topology_status = Instance.MYSQL_STATUS_STANDALONE
    instance.mysql_ddl_dml_eligible = writable
    instance.mysql_ddl_dml_block_reason = (
        "" if writable else _read_only_reason(instance)
    )
    instance.save(update_fields=_INSTANCE_TOPOLOGY_UPDATE_FIELDS)
    _resolve_instance_alerts(instance)


def _upsert_cluster_for_component(component, now):
    primary_candidates = _primary_candidates(component["instances"])
    unmanaged_peers = _unmanaged_primary_peers(component)
    if len(primary_candidates) == 1:
        primary = primary_candidates[0]
        status = MysqlCluster.STATUS_OK
        key = _cluster_key_for_endpoint(primary.host, primary.port)
        primary_instance = primary
    elif len(primary_candidates) > 1:
        status = MysqlCluster.STATUS_AMBIGUOUS_MASTER
        key = _cluster_key_for_component(component)
        primary_instance = None
    else:
        status = MysqlCluster.STATUS_MISSING_MASTER
        key = _cluster_key_for_component(component)
        primary_instance = None

    defaults = {
        "topology_status": status,
        "primary_instance": primary_instance,
        "unmanaged_peers": unmanaged_peers,
        "last_seen_at": now,
    }
    cluster, created = _get_or_create_cluster(
        key, component, primary_instance, defaults
    )
    if not created:
        for field, value in defaults.items():
            setattr(cluster, field, value)
        cluster.save(
            update_fields=[
                "topology_status",
                "primary_instance",
                "unmanaged_peers",
                "last_seen_at",
                "update_time",
            ]
        )
    return cluster


def _get_or_create_cluster(key, component, primary_instance, defaults):
    last_error = None
    for _ in range(5):
        try:
            with transaction.atomic():
                return MysqlCluster.objects.get_or_create(
                    cluster_key=key,
                    defaults={
                        "name": _default_cluster_name(component, primary_instance),
                        "label_value": _unique_label_value(
                            _default_cluster_label(component, primary_instance)
                        ),
                        **defaults,
                    },
                )
        except IntegrityError as exc:
            last_error = exc
            cluster = MysqlCluster.objects.filter(cluster_key=key).first()
            if cluster is not None:
                return cluster, False
    raise last_error or IntegrityError(
        f"Unable to allocate a unique label for MySQL cluster {key!r}."
    )


def _expire_stale_auto_clusters(touched_cluster_ids, now):
    stale_clusters = MysqlCluster.objects.filter(
        membership_source=MysqlCluster.SOURCE_AUTO
    ).exclude(id__in=touched_cluster_ids)
    stale_cluster_ids = list(stale_clusters.values_list("id", flat=True))
    if not stale_cluster_ids:
        return
    stale_clusters.update(
        topology_status=MysqlCluster.STATUS_UNKNOWN,
        primary_instance=None,
        unmanaged_peers=[],
        last_seen_at=None,
        update_time=now,
    )
    MysqlTopologyAlert.objects.filter(
        cluster_id__in=stale_cluster_ids,
        status=MysqlTopologyAlert.STATUS_ACTIVE,
    ).update(status=MysqlTopologyAlert.STATUS_RESOLVED, update_time=now)


def _apply_cluster_component(cluster, instances, now):
    primary_endpoint = (
        _endpoint(cluster.primary_instance.host, cluster.primary_instance.port)
        if cluster.primary_instance_id
        else ""
    )
    for instance in instances:
        if (
            instance.mysql_cluster_membership_source == MysqlCluster.SOURCE_MANUAL
            and instance.mysql_cluster_id
            and instance.mysql_cluster_id != cluster.id
        ):
            if get_mysql_topology_drift_policy() == DRIFT_AUTO_DETACH:
                instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_AUTO
            else:
                _mark_instance_drift(instance, instance.mysql_cluster, now=now)
                continue

        preserve_manual_membership = (
            instance.mysql_cluster_membership_source == MysqlCluster.SOURCE_MANUAL
            and instance.mysql_cluster_id == cluster.id
        )
        instance_endpoint = _endpoint(instance.host, instance.port)
        is_primary = bool(primary_endpoint and instance_endpoint == primary_endpoint)
        group_role = _group_role_for_instance(instance)
        instance.mysql_cluster = cluster
        if not preserve_manual_membership:
            instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_AUTO
        instance.mysql_topology_role = (
            Instance.MYSQL_ROLE_PRIMARY
            if is_primary
            else (
                Instance.MYSQL_ROLE_REPLICA
                if instance.mysql_source_host or group_role == "SECONDARY"
                else Instance.MYSQL_ROLE_UNKNOWN
            )
        )
        if cluster.topology_status == MysqlCluster.STATUS_OK:
            instance.mysql_topology_status = Instance.MYSQL_STATUS_CLUSTERED
            if is_primary:
                instance.mysql_ddl_dml_eligible = _is_writable(instance)
                instance.mysql_ddl_dml_block_reason = (
                    ""
                    if instance.mysql_ddl_dml_eligible
                    else _read_only_reason(instance)
                )
            else:
                instance.mysql_ddl_dml_eligible = False
                instance.mysql_ddl_dml_block_reason = (
                    "DDL/DML must target the cluster master."
                )
        elif cluster.topology_status == MysqlCluster.STATUS_MISSING_MASTER:
            instance.mysql_topology_status = Instance.MYSQL_STATUS_MISSING_MASTER
            instance.mysql_ddl_dml_eligible = False
            instance.mysql_ddl_dml_block_reason = (
                "DDL/DML is blocked because the cluster master is not added to "
                "Datamingle."
            )
            _resolve_instance_alert_type(
                instance, MysqlTopologyAlert.TYPE_AMBIGUOUS_MASTER
            )
            _upsert_cluster_alert(
                cluster,
                MysqlTopologyAlert.TYPE_MISSING_MASTER,
                instance,
                instance.mysql_ddl_dml_block_reason,
            )
        else:
            instance.mysql_topology_status = Instance.MYSQL_STATUS_AMBIGUOUS_MASTER
            instance.mysql_ddl_dml_eligible = False
            instance.mysql_ddl_dml_block_reason = (
                "DDL/DML is blocked because multiple possible cluster masters were "
                "detected."
            )
            _resolve_instance_alert_type(
                instance, MysqlTopologyAlert.TYPE_MISSING_MASTER
            )
            _upsert_cluster_alert(
                cluster,
                MysqlTopologyAlert.TYPE_AMBIGUOUS_MASTER,
                instance,
                instance.mysql_ddl_dml_block_reason,
            )
        instance.save(update_fields=_INSTANCE_TOPOLOGY_UPDATE_FIELDS)
        if cluster.topology_status == MysqlCluster.STATUS_OK:
            _resolve_instance_alerts(instance)


def _mark_instance_drift(instance, cluster, now):
    instance.mysql_topology_status = Instance.MYSQL_STATUS_DRIFT
    instance.mysql_ddl_dml_eligible = (
        get_mysql_topology_drift_policy() == DRIFT_NOTIFY_ONLY
    )
    instance.mysql_ddl_dml_block_reason = (
        ""
        if instance.mysql_ddl_dml_eligible
        else "DDL/DML is blocked because this service has topology drift."
    )
    instance.save(update_fields=_INSTANCE_TOPOLOGY_UPDATE_FIELDS)
    if cluster is not None:
        cluster.topology_status = MysqlCluster.STATUS_DRIFT
        cluster.last_seen_at = now
        cluster.save(update_fields=["topology_status", "last_seen_at", "update_time"])
    _upsert_cluster_alert(
        cluster,
        MysqlTopologyAlert.TYPE_DRIFT,
        instance,
        "MySQL topology drift detected for manually attached service.",
    )


def _upsert_cluster_alert(cluster, alert_type, instance, message):
    MysqlTopologyAlert.objects.update_or_create(
        cluster=cluster,
        instance=instance,
        alert_type=alert_type,
        status=MysqlTopologyAlert.STATUS_ACTIVE,
        defaults={
            "message": message,
            "metadata": {
                "instance_id": instance.id if instance else None,
                "cluster_id": cluster.id if cluster else None,
            },
        },
    )


def _resolve_instance_alerts(instance):
    alert_types = list(
        MysqlTopologyAlert.objects.filter(
            instance=instance,
            status=MysqlTopologyAlert.STATUS_ACTIVE,
        )
        .values_list("alert_type", flat=True)
        .distinct()
    )
    for alert_type in alert_types:
        _resolve_instance_alert_type(instance, alert_type)


def _resolve_instance_alert_type(instance, alert_type):
    active_alert = (
        MysqlTopologyAlert.objects.filter(
            instance=instance,
            alert_type=alert_type,
            status=MysqlTopologyAlert.STATUS_ACTIVE,
        )
        .order_by("-update_time", "-id")
        .first()
    )
    if active_alert is None:
        return
    MysqlTopologyAlert.objects.filter(
        instance=instance,
        alert_type=alert_type,
        status=MysqlTopologyAlert.STATUS_RESOLVED,
    ).delete()
    MysqlTopologyAlert.objects.filter(
        instance=instance,
        alert_type=alert_type,
        status=MysqlTopologyAlert.STATUS_ACTIVE,
    ).exclude(id=active_alert.id).delete()
    active_alert.status = MysqlTopologyAlert.STATUS_RESOLVED
    active_alert.save(update_fields=["status", "update_time"])


def _primary_candidates(instances):
    candidates = []
    seen_endpoints = set()
    for instance in instances:
        if not _is_primary_candidate(instance):
            continue
        endpoint = _endpoint(instance.host, instance.port)
        if endpoint in seen_endpoints:
            continue
        seen_endpoints.add(endpoint)
        candidates.append(instance)
    return candidates


def _is_primary_candidate(instance):
    group_role = _group_role_for_instance(instance)
    if group_role:
        return group_role == "PRIMARY"
    return not instance.mysql_source_host


def _unmanaged_primary_peers(component):
    known_endpoints = {
        _endpoint(instance.host, instance.port) for instance in component["instances"]
    }
    peers = []
    for endpoint in sorted(component["source_endpoints"]):
        if endpoint in known_endpoints:
            continue
        host, port = endpoint.rsplit(":", 1)
        peers.append({"host": host, "port": int(port), "role": "primary"})
    return peers


def _group_member_endpoints(instance):
    endpoints = []
    for member in (
        instance.mysql_topology_details.get("group_replication_members") or []
    ):
        endpoint = _endpoint(
            _member_value(member, "member_host", "MEMBER_HOST"),
            _member_value(member, "member_port", "MEMBER_PORT"),
        )
        if endpoint:
            endpoints.append(endpoint)
    return endpoints


def _group_primary_endpoint(instance):
    for member in (
        instance.mysql_topology_details.get("group_replication_members") or []
    ):
        role = _member_value(member, "member_role", "MEMBER_ROLE").upper()
        if role != "PRIMARY":
            continue
        return _endpoint(
            _member_value(member, "member_host", "MEMBER_HOST"),
            _member_value(member, "member_port", "MEMBER_PORT"),
        )
    return ""


def _group_role_for_instance(instance):
    instance_endpoint = _endpoint(instance.host, instance.port)
    if not instance_endpoint:
        return ""
    for member in (
        instance.mysql_topology_details.get("group_replication_members") or []
    ):
        endpoint = _endpoint(
            _member_value(member, "member_host", "MEMBER_HOST"),
            _member_value(member, "member_port", "MEMBER_PORT"),
        )
        if endpoint != instance_endpoint:
            continue
        return _member_value(member, "member_role", "MEMBER_ROLE").upper()
    return ""


def _member_value(member, *keys):
    if not isinstance(member, dict):
        return ""
    for key in keys:
        value = str(member.get(key) or "").strip()
        if value:
            return value
    return ""


def _cluster_key_for_endpoint(host, port):
    return f"mysql:endpoint:{host}:{port}"


def _cluster_key_for_component(component):
    if component["source_endpoints"]:
        return f"mysql:endpoint:{sorted(component['source_endpoints'])[0]}"
    return f"mysql:component:{sorted(component['endpoints'])[0]}"


def _default_cluster_name(component, primary_instance):
    if primary_instance is not None:
        return primary_instance.instance_name
    endpoint = sorted(component["source_endpoints"] or component["endpoints"])[0]
    return f"MySQL {endpoint}"


def _default_cluster_label(component, primary_instance):
    source = (
        primary_instance.instance_name
        if primary_instance
        else _default_cluster_name(component, None)
    )
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", source.strip().lower()).strip("_")
    return slug or "mysql_cluster"


def _unique_label_value(base):
    candidate = base[:100] or "mysql_cluster"
    suffix = 2
    while MysqlCluster.objects.filter(label_value=candidate).exists():
        tail = f"_{suffix}"
        candidate = f"{base[: 100 - len(tail)]}{tail}"
        suffix += 1
    return candidate


def _read_only_reason(instance):
    if instance.mysql_read_only:
        return "DDL/DML is blocked because MySQL read_only is enabled."
    if instance.mysql_super_read_only:
        return "DDL/DML is blocked because MySQL super_read_only is enabled."
    return "DDL/DML is blocked because this MySQL target is not writable."


def _is_writable(instance):
    return (
        instance.mysql_read_only is False and instance.mysql_super_read_only is not True
    )


def _endpoint(host, port):
    host = str(host or "").strip()
    if not host or not port:
        return ""
    try:
        normalized_port = int(port)
    except (TypeError, ValueError):
        return ""
    return f"{host}:{normalized_port}"


def _nullable_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _nullable_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "on", "true", "yes"}:
        return True
    if normalized in {"0", "off", "false", "no"}:
        return False
    return None


class _DisjointSet:
    def __init__(self):
        self.parent = {}

    @property
    def items(self):
        return self.parent.keys()

    def add(self, item):
        if item:
            self.parent.setdefault(item, item)

    def find(self, item):
        self.add(item)
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left, right):
        if not left or not right:
            return
        self.parent[self.find(right)] = self.find(left)


_INSTANCE_TOPOLOGY_UPDATE_FIELDS = [
    "mysql_cluster",
    "mysql_cluster_membership_source",
    "mysql_topology_role",
    "mysql_topology_status",
    "mysql_ddl_dml_eligible",
    "mysql_ddl_dml_block_reason",
    "update_time",
]
