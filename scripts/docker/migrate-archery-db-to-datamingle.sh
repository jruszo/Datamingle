#!/usr/bin/env bash

set -euo pipefail

MYSQL_CONTAINER="${MYSQL_CONTAINER:-datamingle-mysql}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-123456}"
SOURCE_DB="${SOURCE_DB:-archery}"
TARGET_DB="${TARGET_DB:-datamingle}"
BACKUP_PATH="${BACKUP_PATH:-/tmp/${SOURCE_DB}-to-${TARGET_DB}-$(date +%Y%m%d%H%M%S).sql}"

mysql_exec() {
  docker exec "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$@"
}

database_exists() {
  local database="$1"
  mysql_exec -N -B -e "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${database}';" | grep -qx "$database"
}

table_count() {
  local database="$1"
  mysql_exec -N -B -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${database}';"
}

if ! docker inspect "$MYSQL_CONTAINER" >/dev/null 2>&1; then
  echo "MySQL container '$MYSQL_CONTAINER' was not found." >&2
  echo "Set MYSQL_CONTAINER=mysql when using src/docker-compose/docker-compose.yml." >&2
  exit 1
fi

if ! database_exists "$SOURCE_DB"; then
  echo "Source database '$SOURCE_DB' does not exist in '$MYSQL_CONTAINER'; nothing to migrate." >&2
  exit 1
fi

if database_exists "$TARGET_DB" && [[ "$(table_count "$TARGET_DB")" != "0" ]]; then
  echo "Target database '$TARGET_DB' already exists and is not empty." >&2
  echo "Refusing to overwrite existing data. Back up and clear the target manually if needed." >&2
  exit 1
fi

echo "Writing backup to $BACKUP_PATH"
docker exec "$MYSQL_CONTAINER" mysqldump -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
  --single-transaction --routines --triggers "$SOURCE_DB" > "$BACKUP_PATH"

echo "Creating target database '$TARGET_DB'"
mysql_exec -e "CREATE DATABASE IF NOT EXISTS \`$TARGET_DB\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

echo "Importing '$SOURCE_DB' backup into '$TARGET_DB'"
docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$TARGET_DB" < "$BACKUP_PATH"

echo "Migration copy complete: '$SOURCE_DB' -> '$TARGET_DB'"
echo "Backup retained at $BACKUP_PATH"
