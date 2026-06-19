from django.db import transaction
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver

from api_agents.models import Agent
from api_agents.services import (
    sync_local_node_assignments_for_agent,
    sync_node_assignments_for_instance,
)
from sql.models import Instance


@receiver(pre_save, sender=Instance)
def remember_previous_node(sender, instance, **kwargs):
    if not instance.pk:
        instance._previous_node_id = None
        return

    try:
        previous = sender.objects.only("node_id").get(pk=instance.pk)
    except sender.DoesNotExist:
        instance._previous_node_id = None
        return

    instance._previous_node_id = previous.node_id


@receiver(post_save, sender=Instance)
def sync_instance_node_assignments(sender, instance, update_fields=None, **kwargs):
    if update_fields and set(update_fields).issubset(
        {
            "inventory_status",
            "inventory_last_attempt_at",
            "inventory_last_success_at",
            "inventory_detected_hostname",
            "inventory_detected_version",
        }
    ):
        return

    previous_node_id = getattr(instance, "_previous_node_id", None)
    with transaction.atomic():
        sync_node_assignments_for_instance(instance, previous_node_id=previous_node_id)


@receiver(pre_save, sender=Agent)
def remember_previous_local_node(sender, instance, **kwargs):
    if not instance.pk:
        instance._previous_local_node_id = None
        return

    try:
        previous = sender.objects.only("local_node_id").get(pk=instance.pk)
    except sender.DoesNotExist:
        instance._previous_local_node_id = None
        return

    instance._previous_local_node_id = previous.local_node_id


@receiver(post_save, sender=Agent)
def sync_agent_local_node_assignments(sender, instance, created, **kwargs):
    previous_node_id = getattr(instance, "_previous_local_node_id", None)
    if not created and previous_node_id == instance.local_node_id:
        return
    with transaction.atomic():
        sync_local_node_assignments_for_agent(
            instance, previous_node_id=previous_node_id
        )
