from sql.models import InstanceAccount

SUPPORTED_MANAGEMENT_DB_TYPE = ["mysql", "mongo"]


def get_instanceaccount_unique_value(db_type: str, account: InstanceAccount) -> str:
    """Return the unique value for this instance type from stored account data."""
    if db_type == "mysql":
        return f"`{account['user']}`@`{account['host']}`"
    elif db_type == "mongo":
        return f"{account['db_name']}.{account['user']}"


def get_instanceaccount_unique_key(db_type: str) -> str:
    if db_type == "mysql":
        return "user_host"
    elif db_type == "mongo":
        return "db_name_user"
