# -*- coding:utf-8 -*-
import logging
import math

import sqlparse
from django.forms import model_to_dict
from sqlparse.tokens import Keyword
import pandas as pd

from sql.engines.goinception import GoInceptionEngine
from sql.models import DataMaskingRules, DataMaskingColumns
import re
import traceback

logger = logging.getLogger("default")


def data_masking(instance, db_name, sql, sql_result):
    """Mask sensitive data."""
    try:
        keywords_count = {}
        # Parse query statement and count UNION keywords for special handling.
        p = sqlparse.parse(sql)[0]
        for token in p.tokens:
            if token.ttype is Keyword and token.value.upper() in ["UNION", "UNION ALL"]:
                keywords_count["UNION"] = keywords_count.get("UNION", 0) + 1
        if instance.db_type == "mongo":
            select_list = [
                {
                    "index": index,
                    "field": field,
                    "type": "varchar",
                    "table": "*",
                    "schema": db_name,
                    "alias": field,
                }
                for index, field in enumerate(sql_result.column_list)
            ]
        else:
            # Get select list from goInception.
            inception_engine = GoInceptionEngine()
            select_list = inception_engine.query_data_masking(
                instance=instance, db_name=db_name, sql=sql
            )
        # If UNION exists, call deduplication function.
        select_list = (
            del_repeat(select_list, keywords_count) if keywords_count else select_list
        )
        # Analyze syntax tree to get columns matching masking rules.
        hit_columns = analyze_query_tree(select_list, instance)
        sql_result.mask_rule_hit = True if hit_columns else False
        # Apply masking to columns that match rules.
        masking_rules = {
            i.rule_type: model_to_dict(i) for i in DataMaskingRules.objects.all()
        }
        if hit_columns and sql_result.rows:
            rows = list(sql_result.rows)
            for column in hit_columns:
                index, rule_type = column["index"], column["rule_type"]
                masking_rule = masking_rules.get(rule_type)
                # If default three-segment masking rule does not exist, create it.
                if not masking_rule and rule_type == 100:
                    masking_rule_obj, created = DataMaskingRules.objects.get_or_create(
                        rule_type=100,
                        rule_regex="^([\\s\\S]{0,}?)([\\s\\S]{0,}?)([\\s\\S]{0,}?)$",
                        hide_group=2,
                        rule_desc=(
                            "Three-segment generic masking rule: built-in implementation. "
                            "Regex is not editable for now, hide group is editable."
                        ),
                    )
                    if created:
                        masking_rule = model_to_dict(masking_rule_obj)
                        masking_rules[rule_type] = masking_rule  # Update mapping.
                        masking_rule = masking_rules.get(rule_type)
                if not masking_rule:
                    continue
                for idx, item in enumerate(rows):
                    rows[idx] = list(item)
                    rows[idx][index] = regex(masking_rule, rows[idx][index])
                sql_result.rows = rows
            # Mark result as masked.
            sql_result.is_masked = True
    except Exception as msg:
        logger.warning(f"Data masking exception, details: {traceback.format_exc()}")
        sql_result.error = str(msg)
        sql_result.status = 1
    return sql_result


def del_repeat(select_list, keywords_count):
    """Input data is list result from inception_engine.query_data_masking.
    Before dedup:
    [{'index': 0, 'field': 'phone', 'type': 'varchar(80)', 'table': 'users', 'schema': 'db1', 'alias': 'phone'}, {'index': 1, 'field': 'phone', 'type': 'varchar(80)', 'table': 'users', 'schema': 'db1', 'alias': 'phone'}]
    After dedup:
    [{'index': 0, 'field': 'phone', 'type': 'varchar(80)', 'table': 'users', 'schema': 'db1', 'alias': 'phone'}]
    Returns list in the same structure.
    keywords_count is the occurrence count of keywords.
    """
    # Convert query_tree into DataFrame for easier counting.
    df = pd.DataFrame(select_list)

    # Deduplicate by field only.
    # result_index = df.groupby(['field', 'table', 'schema']).filter(lambda g: len(g) > 1).to_dict('records')
    result_index = df.groupby(["field"]).filter(lambda g: len(g) > 1).to_dict("records")

    # Count duplicates.
    result_len = len(result_index)

    # Compute slice length = duplicates / (union count + 1).
    group_count = int(result_len / (keywords_count["UNION"] + 1))

    result = result_index[:group_count]
    return result


def analyze_query_tree(select_list, instance):
    """Parse select list and return column info matching masking rules."""
    # Get all active masking columns for instance to reduce query loops.
    masking_columns = {
        f"{i.instance}-{i.table_schema}-{i.table_name}-{i.column_name.lower()}": model_to_dict(
            i
        )
        for i in DataMaskingColumns.objects.filter(instance=instance, active=True)
    }
    # Traverse select_list and normalize matched column info.
    hit_columns = []
    for column in select_list:
        table_schema, table, field = (
            column.get("schema"),
            column.get("table"),
            column.get("field"),
        )
        field = field.lower()
        masking_column = masking_columns.get(
            f"{instance}-{table_schema}-{table}-{field}"
        )

        # If not found, try generic wildcard rule.
        if not masking_column:
            masking_column = masking_columns.get(f"{instance}-*-*-{field}")

        if masking_column:
            hit_columns.append(
                {
                    "instance_name": instance.instance_name,
                    "table_schema": table_schema,
                    "table_name": table,
                    "column_name": field,
                    "rule_type": masking_column["rule_type"],
                    "is_hit": True,
                    "index": column["index"],
                }
            )
    return hit_columns


def regex(masking_rule, value):
    """Mask data using regular expression."""
    # If value is null/none/empty string, skip masking.
    if not value:
        return value
    rule_regex = masking_rule["rule_regex"]

    rule_type = masking_rule["rule_type"]
    # Built-in generic rule regex, generated dynamically.
    if rule_type == 100 and isinstance(value, str):
        value_average = math.floor(len(value) / 3)
        value_remainder = len(value) % 3
        value_average_1 = str(value_average)
        value_average_2 = str(value_average + (1 if value_remainder > 0 else 0))
        value_average_3 = str(value_average + (1 if value_remainder > 1 else 0))
        # value_len_str=str(value_len if value_len >= 1 else 1)
        rule_regex = (
            "^([\\s\\S]{"
            + value_average_1
            + ",}?)([\\s\\S]{"
            + value_average_2
            + ",}?)([\\s\\S]{"
            + value_average_3
            + ",}?)$"
        )

    hide_group = masking_rule["hide_group"]
    # Regex must have groups; hidden group is replaced by ****.
    try:
        p = re.compile(rule_regex, re.I)
        m = p.search(str(value))
        masking_str = ""
        if m is None:
            return value
        for i in range(m.lastindex):
            if i == hide_group - 1:
                # Preserve masked length for hidden part.
                group = "*" * len(m.group(i + 1))
            else:
                group = m.group(i + 1)
            masking_str = masking_str + group
        return masking_str
    except AttributeError:
        return value


def brute_mask(instance, sql_result):
    """Input is a resultset.
    sql_result.full_sql
    sql_result.rows query result list, items are tuples.

    Returns sql_result with the same structure, and writes masking errors to
    sql_result.error.
    """
    # Read masking rules for instance and apply them to the whole resultset.
    rule_types = (
        DataMaskingColumns.objects.filter(instance=instance)
        .values_list("rule_type", flat=True)
        .distinct()
    )
    masking_rules = DataMaskingRules.objects.filter(rule_type__in=rule_types)
    for reg in masking_rules:
        compiled_r = re.compile(reg.rule_regex, re.I)
        replace_pattern = r""
        rows = list(sql_result.rows)
        for i in range(1, compiled_r.groups + 1):
            if i == int(reg.hide_group):
                replace_pattern += r"****"
            else:
                replace_pattern += r"\{}".format(i)
        for i in range(len(sql_result.rows)):
            temp_value_list = []
            for j in range(len(sql_result.rows[i])):
                # Apply regex replacement.
                temp_value_list += [
                    compiled_r.sub(replace_pattern, str(sql_result.rows[i][j]))
                ]
            rows[i] = tuple(temp_value_list)
        sql_result.rows = rows
    return sql_result


def simple_column_mask(instance, sql_result):
    """Input is a resultset.
    sql_result.full_sql
    sql_result.rows query result list, items are tuples.
    sql_result.column_list query result columns.
    Returns sql_result with the same structure and writes masking errors to
    sql_result.error.
    """
    # Get masking columns for current instance.
    masking_columns = DataMaskingColumns.objects.filter(instance=instance, active=True)
    # Convert SQL output field names to lowercase for Oracle compatibility.
    sql_result_column_list = [c.lower() for c in sql_result.column_list]
    if masking_columns:
        try:
            for mc in masking_columns:
                # Column name in masking rule.
                column_name = mc.column_name.lower()
                # Matched column indexes for masking.
                _masking_column_index = []
                if column_name in sql_result_column_list:
                    _masking_column_index.append(
                        sql_result_column_list.index(column_name)
                    )
                # Handle alias column masking.
                try:
                    for _c in sql_result_column_list:
                        alias_column_regex = (
                            r'"?([^\s"]+)"?\s+(as\s+)?"?({})[",\s+]?'.format(
                                re.escape(_c)
                            )
                        )
                        alias_column_r = re.compile(alias_column_regex, re.I)
                        # Parse alias field from original SQL.
                        search_data = re.search(alias_column_r, sql_result.full_sql)
                        # Field name.
                        _column_name = search_data.group(1).lower()
                        s_column_name = re.sub(r'^"?\w+"?\."?|\.|"$', "", _column_name)
                        # Alias.
                        alias_name = search_data.group(3).lower()
                        # If field name matches masking config, mask this alias field.
                        if s_column_name == column_name:
                            _masking_column_index.append(
                                sql_result_column_list.index(alias_name)
                            )
                except:
                    pass

                for masking_column_index in _masking_column_index:
                    # Masking rule.
                    masking_rule = DataMaskingRules.objects.get(rule_type=mc.rule_type)
                    # Replacement pattern after masking.
                    compiled_r = re.compile(masking_rule.rule_regex, re.I | re.S)
                    replace_pattern = r""
                    for i in range(1, compiled_r.groups + 1):
                        if i == int(masking_rule.hide_group):
                            replace_pattern += r"****"
                        else:
                            replace_pattern += r"\{}".format(i)

                    rows = list(sql_result.rows)
                    for i in range(len(sql_result.rows)):
                        temp_value_list = []
                        for j in range(len(sql_result.rows[i])):
                            column_data = sql_result.rows[i][j]
                            if j == masking_column_index:
                                column_data = compiled_r.sub(
                                    replace_pattern, str(sql_result.rows[i][j])
                                )
                            temp_value_list += [column_data]
                        rows[i] = tuple(temp_value_list)
                    sql_result.rows = rows
        except Exception as e:
            sql_result.error = str(e)

    return sql_result
