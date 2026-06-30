from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from common.config import SysConfig
from sql.models import Config, Instance, MysqlCluster, MysqlTopologyAlert
from sql.mysql_topology import apply_mysql_topology_snapshot, reconcile_mysql_topology
from sql.inventory import refresh_instance_inventory_snapshot


class MysqlTopologyTests(TestCase):
    def _mysql_instance(self, name, host, port=3306):
        return Instance.objects.create(
            instance_name=name,
            type="master",
            db_type="mysql",
            host=host,
            port=port,
            user="root",
            password="secret",
            workflow_enabled=True,
        )

    def _set_drift_policy(self, policy):
        config = SysConfig()
        original_policy = config.get("mysql_topology_drift_policy", None)
        if original_policy is None:
            self.addCleanup(
                Config.objects.filter(item="mysql_topology_drift_policy").delete
            )
        else:
            self.addCleanup(config.set, "mysql_topology_drift_policy", original_policy)
        config.set("mysql_topology_drift_policy", policy)

    def test_new_mysql_instance_defaults_to_not_ddl_dml_eligible(self):
        instance = self._mysql_instance("mysql-unknown", "10.0.0.20")

        self.assertFalse(instance.mysql_ddl_dml_eligible)
        self.assertEqual(instance.mysql_topology_status, Instance.MYSQL_STATUS_UNKNOWN)

    def test_cluster_master_is_eligible_and_replica_is_blocked(self):
        primary = self._mysql_instance("mysql-primary", "10.0.0.10")
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        apply_mysql_topology_snapshot(
            primary,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
                "source_host": "",
                "source_port": None,
            },
        )
        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.10",
                "source_port": 3306,
            },
        )

        primary.refresh_from_db()
        replica.refresh_from_db()

        self.assertIsNotNone(primary.mysql_cluster_id)
        self.assertEqual(primary.mysql_cluster_id, replica.mysql_cluster_id)
        self.assertEqual(primary.mysql_cluster.primary_instance_id, primary.id)
        self.assertEqual(primary.mysql_cluster.topology_status, MysqlCluster.STATUS_OK)
        self.assertEqual(primary.mysql_topology_role, Instance.MYSQL_ROLE_PRIMARY)
        self.assertEqual(replica.mysql_topology_role, Instance.MYSQL_ROLE_REPLICA)
        self.assertTrue(primary.mysql_ddl_dml_eligible)
        self.assertFalse(replica.mysql_ddl_dml_eligible)
        self.assertIn("master", replica.mysql_ddl_dml_block_reason)

    def test_older_snapshot_does_not_overwrite_newer_topology(self):
        instance = self._mysql_instance("mysql-primary", "10.0.0.10")
        newer_seen_at = timezone.now()
        older_seen_at = newer_seen_at - timedelta(seconds=30)

        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
            },
            now=newer_seen_at,
        )
        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "stale-uuid",
                "read_only": True,
                "super_read_only": True,
            },
            now=older_seen_at,
        )

        instance.refresh_from_db()
        self.assertEqual(instance.mysql_server_uuid, "primary-uuid")
        self.assertFalse(instance.mysql_read_only)
        self.assertLess(
            abs((instance.mysql_topology_last_seen_at - newer_seen_at).total_seconds()),
            1,
        )

    def test_missing_master_cluster_blocks_replica_and_records_unmanaged_peer(self):
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.99",
                "source_port": 3306,
            },
        )

        replica.refresh_from_db()

        self.assertIsNotNone(replica.mysql_cluster_id)
        self.assertIsNone(replica.mysql_cluster.primary_instance_id)
        self.assertEqual(
            replica.mysql_cluster.topology_status, MysqlCluster.STATUS_MISSING_MASTER
        )
        self.assertEqual(
            replica.mysql_cluster.unmanaged_peers,
            [{"host": "10.0.0.99", "port": 3306, "role": "primary"}],
        )
        self.assertFalse(replica.mysql_ddl_dml_eligible)
        self.assertIn("master is not added", replica.mysql_ddl_dml_block_reason)

    def test_primary_host_uses_primary_port_when_source_port_is_absent(self):
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "primary_host": "10.0.0.99",
                "primary_port": 3310,
            },
        )

        replica.refresh_from_db()

        self.assertEqual(
            replica.mysql_cluster.unmanaged_peers,
            [{"host": "10.0.0.99", "port": 3310, "role": "primary"}],
        )

    def test_read_only_known_primary_keeps_primary_status_but_blocks_writes(self):
        primary = self._mysql_instance("mysql-primary", "10.0.0.10")
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        apply_mysql_topology_snapshot(
            primary,
            {
                "server_uuid": "primary-uuid",
                "read_only": True,
                "super_read_only": False,
            },
        )
        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.10",
                "source_port": 3306,
            },
        )

        primary.refresh_from_db()
        replica.refresh_from_db()

        self.assertEqual(primary.mysql_cluster.primary_instance_id, primary.id)
        self.assertEqual(primary.mysql_cluster.topology_status, MysqlCluster.STATUS_OK)
        self.assertEqual(primary.mysql_topology_role, Instance.MYSQL_ROLE_PRIMARY)
        self.assertFalse(primary.mysql_ddl_dml_eligible)
        self.assertIn("read_only", primary.mysql_ddl_dml_block_reason)
        self.assertEqual(replica.mysql_cluster_id, primary.mysql_cluster_id)

    def test_malformed_group_member_port_does_not_abort_reconciliation(self):
        secondary = self._mysql_instance("mysql-secondary", "10.0.0.11")

        apply_mysql_topology_snapshot(
            secondary,
            {
                "server_uuid": "secondary-uuid",
                "read_only": True,
                "super_read_only": True,
                "group_replication_members": [
                    {
                        "member_host": "10.0.0.10",
                        "member_port": "not-a-port",
                        "member_role": "PRIMARY",
                    }
                ],
            },
        )

        secondary.refresh_from_db()

        self.assertEqual(
            secondary.mysql_topology_status, Instance.MYSQL_STATUS_STANDALONE
        )
        self.assertFalse(secondary.mysql_ddl_dml_eligible)

    def test_group_replication_secondary_without_known_primary_is_blocked(self):
        secondary = self._mysql_instance("mysql-secondary", "10.0.0.11")

        apply_mysql_topology_snapshot(
            secondary,
            {
                "server_uuid": "secondary-uuid",
                "read_only": True,
                "super_read_only": True,
                "group_replication_members": [
                    {
                        "member_host": "10.0.0.10",
                        "member_port": "3306",
                        "member_role": "PRIMARY",
                        "member_state": "ONLINE",
                    },
                    {
                        "member_host": "10.0.0.11",
                        "member_port": "3306",
                        "member_role": "SECONDARY",
                        "member_state": "ONLINE",
                    },
                ],
            },
        )

        secondary.refresh_from_db()

        self.assertEqual(
            secondary.mysql_cluster.topology_status,
            MysqlCluster.STATUS_MISSING_MASTER,
        )
        self.assertEqual(
            secondary.mysql_cluster.unmanaged_peers,
            [{"host": "10.0.0.10", "port": 3306, "role": "primary"}],
        )
        self.assertEqual(secondary.mysql_topology_role, Instance.MYSQL_ROLE_REPLICA)
        self.assertFalse(secondary.mysql_ddl_dml_eligible)

    def test_source_endpoint_without_instance_endpoint_does_not_abort_reconciliation(
        self,
    ):
        instance = self._mysql_instance("mysql-unclassified", "")

        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "unclassified-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.99",
                "source_port": 3306,
            },
        )

        instance.refresh_from_db()

        self.assertEqual(
            instance.mysql_topology_status, Instance.MYSQL_STATUS_STANDALONE
        )
        self.assertFalse(instance.mysql_ddl_dml_eligible)

    def test_duplicate_primary_endpoint_records_stay_in_same_cluster(self):
        primary_a = self._mysql_instance("mysql-primary-a", "10.0.0.10")
        primary_b = self._mysql_instance("mysql-primary-b", "10.0.0.10")
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        primary_snapshot = {
            "server_uuid": "primary-uuid",
            "read_only": False,
            "super_read_only": False,
        }
        apply_mysql_topology_snapshot(primary_a, primary_snapshot)
        apply_mysql_topology_snapshot(primary_b, primary_snapshot)
        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.10",
                "source_port": 3306,
            },
        )

        primary_a.refresh_from_db()
        primary_b.refresh_from_db()
        replica.refresh_from_db()

        self.assertEqual(primary_a.mysql_cluster_id, primary_b.mysql_cluster_id)
        self.assertEqual(primary_a.mysql_cluster_id, replica.mysql_cluster_id)
        self.assertEqual(primary_a.mysql_topology_role, Instance.MYSQL_ROLE_PRIMARY)
        self.assertEqual(primary_b.mysql_topology_role, Instance.MYSQL_ROLE_PRIMARY)
        self.assertTrue(primary_a.mysql_ddl_dml_eligible)
        self.assertTrue(primary_b.mysql_ddl_dml_eligible)
        self.assertFalse(replica.mysql_ddl_dml_eligible)

    def test_disappeared_auto_cluster_is_marked_unknown(self):
        primary = self._mysql_instance("mysql-primary", "10.0.0.10")
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        apply_mysql_topology_snapshot(
            primary,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
            },
        )
        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.10",
                "source_port": 3306,
            },
        )
        primary.refresh_from_db()
        cluster = primary.mysql_cluster
        self.assertEqual(cluster.topology_status, MysqlCluster.STATUS_OK)

        apply_mysql_topology_snapshot(primary, {})
        apply_mysql_topology_snapshot(replica, {})

        primary.refresh_from_db()
        replica.refresh_from_db()
        cluster.refresh_from_db()
        self.assertIsNone(primary.mysql_cluster_id)
        self.assertIsNone(replica.mysql_cluster_id)
        self.assertEqual(cluster.topology_status, MysqlCluster.STATUS_UNKNOWN)
        self.assertIsNone(cluster.primary_instance_id)
        self.assertIsNone(cluster.last_seen_at)

    def test_auto_cluster_expires_when_no_instances_are_reportable(self):
        now = timezone.now()
        cluster = MysqlCluster.objects.create(
            name="payments",
            label_value="payments",
            cluster_key="mysql:endpoint:10.0.0.10:3306",
            topology_status=MysqlCluster.STATUS_OK,
            last_seen_at=now - timedelta(hours=1),
        )
        MysqlTopologyAlert.objects.create(
            cluster=cluster,
            alert_type=MysqlTopologyAlert.TYPE_MISSING_MASTER,
            status=MysqlTopologyAlert.STATUS_ACTIVE,
            message="Cluster master is missing.",
        )

        reconcile_mysql_topology(now=now)

        cluster.refresh_from_db()
        self.assertEqual(cluster.topology_status, MysqlCluster.STATUS_UNKNOWN)
        self.assertIsNone(cluster.last_seen_at)
        self.assertFalse(
            MysqlTopologyAlert.objects.filter(
                cluster=cluster, status=MysqlTopologyAlert.STATUS_ACTIVE
            ).exists()
        )

    def test_alert_type_switch_resolves_stale_alerts_for_instance(self):
        instance = self._mysql_instance("mysql-a", "10.0.0.11")
        peer = self._mysql_instance("mysql-b", "10.0.0.12")

        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "mysql-a",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.99",
                "source_port": 3306,
            },
        )
        self.assertTrue(
            MysqlTopologyAlert.objects.filter(
                instance=instance,
                alert_type=MysqlTopologyAlert.TYPE_MISSING_MASTER,
                status=MysqlTopologyAlert.STATUS_ACTIVE,
            ).exists()
        )

        group_members = [
            {
                "member_host": "10.0.0.11",
                "member_port": "3306",
                "member_role": "PRIMARY",
            },
            {
                "member_host": "10.0.0.12",
                "member_port": "3306",
                "member_role": "PRIMARY",
            },
        ]
        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "mysql-a",
                "read_only": False,
                "super_read_only": False,
                "group_replication_members": group_members,
            },
        )
        apply_mysql_topology_snapshot(
            peer,
            {
                "server_uuid": "mysql-b",
                "read_only": False,
                "super_read_only": False,
                "group_replication_members": group_members,
            },
        )

        self.assertFalse(
            MysqlTopologyAlert.objects.filter(
                instance=instance,
                alert_type=MysqlTopologyAlert.TYPE_MISSING_MASTER,
                status=MysqlTopologyAlert.STATUS_ACTIVE,
            ).exists()
        )
        self.assertTrue(
            MysqlTopologyAlert.objects.filter(
                instance=instance,
                alert_type=MysqlTopologyAlert.TYPE_AMBIGUOUS_MASTER,
                status=MysqlTopologyAlert.STATUS_ACTIVE,
            ).exists()
        )

    def test_manual_cluster_drift_creates_alert_and_blocks_workflows(self):
        self._set_drift_policy("notify_block")
        cluster = MysqlCluster.objects.create(
            name="payments",
            label_value="payments",
            cluster_key="manual:payments",
            membership_source=MysqlCluster.SOURCE_MANUAL,
        )
        instance = self._mysql_instance("mysql-primary", "10.0.0.10")
        instance.mysql_cluster = cluster
        instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_MANUAL
        instance.save(
            update_fields=[
                "mysql_cluster",
                "mysql_cluster_membership_source",
                "update_time",
            ]
        )

        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
                "source_host": "",
                "source_port": None,
            },
        )

        instance.refresh_from_db()
        cluster.refresh_from_db()

        self.assertEqual(instance.mysql_topology_status, Instance.MYSQL_STATUS_DRIFT)
        self.assertFalse(instance.mysql_ddl_dml_eligible)
        self.assertIn("topology drift", instance.mysql_ddl_dml_block_reason)
        self.assertTrue(
            MysqlTopologyAlert.objects.filter(
                instance=instance,
                cluster=cluster,
                status=MysqlTopologyAlert.STATUS_ACTIVE,
                alert_type=MysqlTopologyAlert.TYPE_DRIFT,
            ).exists()
        )

    def test_manual_membership_is_preserved_when_detected_cluster_matches(self):
        primary = self._mysql_instance("mysql-primary", "10.0.0.10")
        replica = self._mysql_instance("mysql-replica", "10.0.0.11")

        apply_mysql_topology_snapshot(
            primary,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
            },
        )
        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.10",
                "source_port": 3306,
            },
        )
        replica.refresh_from_db()
        cluster_id = replica.mysql_cluster_id
        replica.mysql_cluster_membership_source = MysqlCluster.SOURCE_MANUAL
        replica.save(update_fields=["mysql_cluster_membership_source", "update_time"])

        apply_mysql_topology_snapshot(
            replica,
            {
                "server_uuid": "replica-uuid",
                "read_only": True,
                "super_read_only": True,
                "source_host": "10.0.0.10",
                "source_port": 3306,
            },
        )

        replica.refresh_from_db()
        self.assertEqual(replica.mysql_cluster_id, cluster_id)
        self.assertEqual(
            replica.mysql_cluster_membership_source, MysqlCluster.SOURCE_MANUAL
        )

    def test_auto_detach_drift_policy_detaches_manual_standalone_service(self):
        self._set_drift_policy("auto_detach")
        cluster = MysqlCluster.objects.create(
            name="payments",
            label_value="payments",
            cluster_key="manual:payments",
            membership_source=MysqlCluster.SOURCE_MANUAL,
        )
        instance = self._mysql_instance("mysql-primary", "10.0.0.10")
        instance.mysql_cluster = cluster
        instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_MANUAL
        instance.save(
            update_fields=[
                "mysql_cluster",
                "mysql_cluster_membership_source",
                "update_time",
            ]
        )

        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
            },
        )

        instance.refresh_from_db()
        self.assertIsNone(instance.mysql_cluster_id)
        self.assertEqual(
            instance.mysql_cluster_membership_source, MysqlCluster.SOURCE_AUTO
        )
        self.assertEqual(
            instance.mysql_topology_status, Instance.MYSQL_STATUS_STANDALONE
        )
        self.assertTrue(instance.mysql_ddl_dml_eligible)
        self.assertFalse(
            MysqlTopologyAlert.objects.filter(
                instance=instance,
                status=MysqlTopologyAlert.STATUS_ACTIVE,
                alert_type=MysqlTopologyAlert.TYPE_DRIFT,
            ).exists()
        )

    def test_notify_only_drift_policy_allows_manual_service_with_alert(self):
        self._set_drift_policy("notify_only")
        cluster = MysqlCluster.objects.create(
            name="payments",
            label_value="payments",
            cluster_key="manual:payments",
            membership_source=MysqlCluster.SOURCE_MANUAL,
        )
        instance = self._mysql_instance("mysql-primary", "10.0.0.10")
        instance.mysql_cluster = cluster
        instance.mysql_cluster_membership_source = MysqlCluster.SOURCE_MANUAL
        instance.save(
            update_fields=[
                "mysql_cluster",
                "mysql_cluster_membership_source",
                "update_time",
            ]
        )

        apply_mysql_topology_snapshot(
            instance,
            {
                "server_uuid": "primary-uuid",
                "read_only": False,
                "super_read_only": False,
            },
        )

        instance.refresh_from_db()
        cluster.refresh_from_db()
        self.assertEqual(instance.mysql_topology_status, Instance.MYSQL_STATUS_DRIFT)
        self.assertTrue(instance.mysql_ddl_dml_eligible)
        self.assertEqual(instance.mysql_ddl_dml_block_reason, "")
        self.assertEqual(cluster.topology_status, MysqlCluster.STATUS_DRIFT)
        self.assertTrue(
            MysqlTopologyAlert.objects.filter(
                instance=instance,
                cluster=cluster,
                status=MysqlTopologyAlert.STATUS_ACTIVE,
                alert_type=MysqlTopologyAlert.TYPE_DRIFT,
            ).exists()
        )

    @patch("sql.inventory.apply_mysql_topology_snapshot")
    @patch("sql.inventory.collect_inventory_snapshot")
    def test_inventory_marks_failed_when_topology_reconciliation_fails(
        self, collect_inventory_snapshot, apply_snapshot
    ):
        instance = self._mysql_instance("mysql-primary", "10.0.0.10")
        collect_inventory_snapshot.return_value = {
            "hostname": "mysql-primary",
            "version": "8.0.36",
            "mysql_topology": {"read_only": False, "super_read_only": False},
        }
        apply_snapshot.side_effect = RuntimeError("topology failed")

        result = refresh_instance_inventory_snapshot(instance)

        instance.refresh_from_db()
        self.assertFalse(result["success"])
        self.assertEqual(instance.inventory_status, Instance.INVENTORY_STATUS_FAILED)
        self.assertIn("topology failed", result["error"])

    @patch("sql.inventory.apply_mysql_topology_snapshot")
    @patch("sql.inventory.collect_inventory_snapshot")
    def test_inventory_applies_empty_mysql_topology_snapshot(
        self, collect_inventory_snapshot, apply_snapshot
    ):
        instance = self._mysql_instance("mysql-primary", "10.0.0.10")
        now = timezone.now()
        collect_inventory_snapshot.return_value = {
            "hostname": "mysql-primary",
            "version": "8.0.36",
        }

        result = refresh_instance_inventory_snapshot(instance, now=now)

        self.assertTrue(result["success"])
        apply_snapshot.assert_called_once_with(instance, {}, now=now)
