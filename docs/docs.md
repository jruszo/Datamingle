# MySQL Database Design Guidelines (Reference)

## Table of Contents
1. Background and Goal
2. Design Guidelines
2.1 Database Design
2.1.1 Database Name
2.1.2 Table Structure
2.1.3 Column Type Optimization
2.1.4 Index Design
2.1.5 Sharding and Partitioning
2.1.6 Character Set
2.1.7 DAO Layer Recommendations
2.1.8 Example of a Standard CREATE TABLE
2.2 SQL Writing
2.2.1 DML Statements
2.2.2 Multi-Table Joins
2.2.3 Transactions
2.2.4 Sorting and Grouping
2.2.5 SQL Forbidden in Production

## 1. Background and Goal

Compared with Oracle and SQL Server, MySQL has both strengths and weaknesses at the kernel level.
To use MySQL effectively, teams should follow engineering standards that leverage its strengths and avoid known pitfalls.
This document helps RD/QA/OP teams make production-ready database decisions across schema design, change management, and SQL writing.

## 2. Design Guidelines

### 2.1 Database Design

Rules are tagged as `[High Risk]`, `[Mandatory]`, and `[Recommended]`, in descending priority.

Designs that violate `[High Risk]` or `[Mandatory]` should be rejected and revised.

### 2.1.1 Database Name

1. `[Mandatory]` Database names must be within 32 characters. Related module tables should reflect join relationships (for example, `user` and `user_login`).
2. `[Mandatory]` Name format should be `business_system_subsystem`. Within the same module, use consistent table prefixes.
3. `[Mandatory]` Common sharded naming format is `db_pattern_number` (starting at `0`, for example `wenda_001`); time-based sharding can use `db_pattern_time`.
4. `[Mandatory]` Explicitly specify character set when creating a database. Use only `utf8` or `utf8mb4`, for example: `create database db1 default character set utf8;`.

### 2.1.2 Table Structure

1. `[Mandatory]` Table/column names must be within 32 characters. Table names must use lowercase letters, digits, and underscores only.
2. `[Mandatory]` Table names should strongly reflect module semantics and use module prefixes consistently.
3. `[Mandatory]` Explicitly specify table character set (`utf8` or `utf8mb4`).
4. `[Mandatory]` Explicitly specify storage engine. Use `InnoDB` by default. Non-InnoDB/MyISAM/Memory engines must be DBA-approved for production.
5. `[Mandatory]` Every table must include table/column comments.
6. `[Recommended]` Primary key guidance: use `id` (`int`/`bigint`, `auto_increment`) as the PK. Avoid random business keys as PKs; use business IDs (`user_id`, `order_id`) with `unique key`.
7. `[Recommended]` Core tables should include `create_time` and `update_time`.
8. `[Recommended]` Prefer `NOT NULL` for all columns, with suitable `DEFAULT` values.
9. `[Recommended]` Vertically split large fields (`blob`, `text`) into separate tables.
10. `[Recommended]` Use denormalization for frequent joins where appropriate.
11. `[Mandatory]` Intermediate tables must start with `tmp_`, backup/snapshot tables with `bak_`; clean both regularly.
12. `[Mandatory]` `ALTER TABLE` on very large tables (for example over 1M rows) must be DBA-approved and run during off-peak windows.

### 2.1.3 Column Type Optimization

1. `[Recommended]` Use `bigint` for auto-increment IDs to reduce overflow risk.
2. `[Recommended]` Low-cardinality fields (for example status/type) should prefer `tinyint`/`smallint`.
3. `[Recommended]` Prefer integer storage for IP values (`inet_aton` / `inet_ntoa`) over `char(15)`.
4. `[Recommended]` Avoid `enum`/`set` where possible; prefer numeric types.
5. `[Recommended]` Use `blob`/`text` only when necessary due memory/I/O overhead.
6. `[Recommended]` For money fields, prefer integer cents over floating-point.
7. `[Recommended]` Prefer `varchar` over `char`; avoid oversized text fields.
8. `[Recommended]` Prefer `timestamp` when range is sufficient; use epoch `int` storage if needed.

### 2.1.4 Index Design

1. `[Mandatory]` InnoDB tables must use `id int/bigint auto_increment` as PK; PK value must not be updated.
2. `[Recommended]` Naming: PK `pk_`, unique `uk_`/`uq_`, normal index `idx_`.
3. `[Mandatory]` InnoDB/MyISAM indexes must be `BTREE`; MEMORY can use `HASH` or `BTREE`.
4. `[Mandatory]` Single index record length must not exceed 64KB.
5. `[Recommended]` Keep index count per table limited (for example no more than 7).
6. `[Recommended]` Prefer composite indexes; place most selective column first.
7. `[Recommended]` Ensure driven-table join columns are indexed.
8. `[Recommended]` Remove redundant indexes (for example existing `key(a,b)` makes `key(a)` redundant).

### 2.1.5 Sharding and Partitioning

1. `[Mandatory]` Partition key must be indexed or be the first column in a composite index.
2. `[Mandatory]` Total partition count per table (including subpartitions) must not exceed 1024.
3. `[Mandatory]` Define partition creation and cleanup strategy before release.
4. `[Mandatory]` SQL against partition tables must include partition key.
5. `[Recommended]` Keep partition files and total size bounded; keep partition count manageable.
6. `[Mandatory]` Run partition-table `ALTER TABLE` in off-peak windows.
7. `[Mandatory]` Sharded database count should be controlled.
8. `[Mandatory]` Sharded table count should be controlled.
9. `[Recommended]` Keep single-shard table size/row count within operationally safe limits.
10. `[Recommended]` Prefer modulo-based horizontal sharding; use date-based sharding for logs/reporting data.

### 2.1.6 Character Set

1. `[Mandatory]` Database/table/column character sets must be consistent (`utf8` or `utf8mb4`).
2. `[Mandatory]` Application charset and DB charset must be consistent.

### 2.1.7 DAO Layer Recommendations

1. `[Recommended]` Prefer explicit SQL + bound parameters in performance-critical paths; avoid heavy ORM misuse.
2. `[Recommended]` DB/Redis clients must have timeout and reconnect policies with retry intervals.
3. `[Recommended]` Surface original DB/Redis error details in logs for easier troubleshooting.
4. `[Recommended]` Configure connection pools (initial/min/max/timeout/recycle) according to workload.
5. `[Recommended]` Define cleanup/archive strategy for log/history tables before launch.
6. `[Recommended]` Consider replication lag impact in architecture; route strongly consistent reads appropriately.
7. `[Recommended]` Updates should prefer PK-based conditions to reduce lock contention.
8. `[Recommended]` Keep lock acquisition order consistent to avoid deadlocks.
9. `[Recommended]` Move hot rows/columns to cache (for example Memcached/Redis) when suitable.

### 2.1.8 Example of a Standard CREATE TABLE

```sql
CREATE TABLE user (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(11) NOT NULL COMMENT 'user id',
  `username` varchar(45) NOT NULL COMMENT 'real name',
  `email` varchar(30) NOT NULL COMMENT 'user email',
  `nickname` varchar(45) NOT NULL COMMENT 'nickname',
  `avatar` int(11) NOT NULL COMMENT 'avatar',
  `birthday` date NOT NULL COMMENT 'birthday',
  `sex` tinyint(4) DEFAULT '0' COMMENT 'gender',
  `short_introduce` varchar(150) DEFAULT NULL COMMENT 'short self introduction',
  `user_resume` varchar(300) NOT NULL COMMENT 'resume storage path',
  `user_register_ip` int NOT NULL COMMENT 'registration source ip',
  `create_time` timestamp NOT NULL COMMENT 'record creation time',
  `update_time` timestamp NOT NULL COMMENT 'record update time',
  `user_review_status` tinyint NOT NULL COMMENT 'profile review status',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_user_id` (`user_id`),
  KEY `idx_username`(`username`),
  KEY `idx_create_time`(`create_time`,`user_review_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='website user basic information';
```

## 2.2 SQL Writing

### 2.2.1 DML Statements

1. `[Mandatory]` `SELECT` must specify explicit columns; avoid `select *`.
2. `[Mandatory]` `INSERT` must specify explicit columns; avoid positional `insert into t values (...)`.
3. `[Recommended]` Limit batch insert value count (for example <= 5000).
4. `[Recommended]` Prefer `UNION ALL` over `UNION` unless dedup is required.
5. `[Recommended]` Keep `IN (...)` lists bounded (for example <= 500).
6. `[Recommended]` For large transactional updates, split into smaller batches with sleep intervals.
7. `[Mandatory]` Transactional tables must be InnoDB.
8. `[Mandatory]` Writes/transactions go to primary; read-only SQL goes to replicas.
9. `[Mandatory]` DML must include indexed `WHERE` (except tiny static tables).
10. `[Mandatory]` Avoid production `hint`s unless strongly justified.
11. `[Mandatory]` Ensure type consistency across comparison operands.
12. `[Recommended]` `SELECT|UPDATE|DELETE|REPLACE` should include indexed `WHERE`.
13. `[Mandatory]` Avoid full table scans on large production tables.
14. `[Mandatory]` Avoid pure fuzzy `LIKE` predicates without other selective conditions.
15. `[Recommended]` Avoid functions/expressions on indexed columns in predicates.
16. `[Recommended]` Reduce `OR`; rewrite as `UNION` if it improves index usage.
17. `[Recommended]` For deep pagination, filter by indexed cursor key first.

### 2.2.2 Multi-Table Joins

1. `[Mandatory]` Avoid cross-database joins.
2. `[Mandatory]` Avoid joins in update-class business SQL.
3. `[Recommended]` Prefer join or split-query strategy over complex subqueries.
4. `[Recommended]` Limit join table count in production.
5. `[Recommended]` Use aliases and explicit qualified fields.
6. `[Recommended]` Choose small, selective driving sets where possible.

### 2.2.3 Transactions

1. `[Recommended]` Keep rows affected and `IN` argument counts bounded in one transaction.
2. `[Recommended]` Use interval control (sleep) in batch jobs.
3. `[Recommended]` Control concurrent insert volume for `auto_increment` tables.
4. `[Mandatory]` Explicitly consider transaction isolation impact (dirty/non-repeatable/phantom reads).
5. `[Recommended]` Keep transaction SQL count small (except special business flows like payment).
6. `[Recommended]` Prefer PK/unique-key update predicates to reduce lock range and deadlocks.
7. `[Recommended]` Move external calls outside transactions.
8. `[Recommended]` For strong consistency under replication lag, force reads to primary within transaction.

### 2.2.4 Sorting and Grouping

1. `[Recommended]` Minimize `order by` if business permits; offload sorting to application when appropriate.
2. `[Recommended]` Use indexes to satisfy `order by`/`group by`/`distinct` when possible.
3. `[Recommended]` Keep filtered result sets small before sort/group/distinct.

### 2.2.5 SQL Forbidden in Production

1. `[High Risk]` Avoid `update|delete ... where ... limit ...` that may cause primary-replica inconsistency.
2. `[High Risk]` Avoid inefficient correlated updates/subqueries in write paths.
3. `[Mandatory]` Avoid procedure/function/trigger/view/event/foreign-key constraints when they harm scalability.
4. `[Mandatory]` Avoid high-concurrency `insert ... on duplicate key update ...` without strict evaluation.
5. `[Mandatory]` Avoid join-based update statements in production.
