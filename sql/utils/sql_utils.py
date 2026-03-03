# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: sql_utils.py
@time: 2019/03/13
"""

import re
import xml
import mybatis_mapper2sql
import sqlparse

from sql.engines.models import SqlItem
from sql.utils.extract_tables import extract_tables as extract_tables_by_sql_parse

__author__ = "hhyo"


def get_syntax_type(sql, parser=True, db_type="mysql"):
    """
    Return SQL statement type, only distinguishing DDL and DML.
    :param sql:
    :param parser: Whether to use sqlparse for parsing.
    :param db_type: Required when sqlparse parsing is disabled.
    :return:
    """
    sql = remove_comments(sql=sql, db_type=db_type)
    if parser:
        try:
            statement = sqlparse.parse(sql)[0]
            syntax_type = statement.token_first(skip_cm=True).ttype.__str__()
            if syntax_type == "Token.Keyword.DDL":
                syntax_type = "DDL"
            elif syntax_type == "Token.Keyword.DML":
                syntax_type = "DML"
        except Exception:
            syntax_type = None
    else:
        if db_type == "mysql":
            ddl_re = r"^alter|^create|^drop|^rename|^truncate"
            dml_re = r"^call|^delete|^do|^handler|^insert|^load\s+data|^load\s+xml|^replace|^select|^update"
        elif db_type == "oracle":
            ddl_re = r"^alter|^create|^drop|^rename|^truncate"
            dml_re = r"^delete|^exec|^insert|^select|^update|^with|^merge"
        else:
            # TODO: parsing regex for other databases.
            return None
        if re.match(ddl_re, sql, re.I):
            syntax_type = "DDL"
        elif re.match(dml_re, sql, re.I):
            syntax_type = "DML"
        else:
            syntax_type = None
    return syntax_type


def remove_comments(sql, db_type="mysql"):
    """
    Remove comments from SQL statements.
    Source: https://stackoverflow.com/questions/35647841/parse-sql-file-with-comments-into-sqlite-with-python
    :param sql:
    :param db_type:
    :return:
    """
    sql_comments_re = {
        "oracle": [r"(?:--)[^\n]*\n", r"(?:\W|^)(?:remark|rem)\s+[^\n]*\n"],
        "mysql": [r"(?:#|--\s)[^\n]*\n"],
    }
    specific_comment_re = sql_comments_re[db_type]
    additional_patterns = "|"
    if isinstance(specific_comment_re, str):
        additional_patterns += specific_comment_re
    elif isinstance(specific_comment_re, list):
        additional_patterns += "|".join(specific_comment_re)
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/{})".format(additional_patterns)
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        if match.group(2):
            return ""
        else:
            return match.group(1)

    return regex.sub(_replacer, sql).strip()


def extract_tables(sql):
    """
    Get schema/table names referenced in SQL.
    :param sql:
    :return:
    """
    tables = list()
    for i in extract_tables_by_sql_parse(sql):
        tables.append(
            {
                "schema": i.schema,
                "name": i.name,
            }
        )
    return tables


def generate_sql(text):
    """
    Parse SQL list from SQL text or MyBatis3 Mapper XML file.
    :param text:
    :return: [{"sql_id": key, "sql": soar.compress(value)}]
    """
    # Try XML parsing first.
    try:
        mapper, xml_raw_text = mybatis_mapper2sql.create_mapper(xml_raw_text=text)
        statements = mybatis_mapper2sql.get_statement(mapper, result_type="list")
        rows = []
        # Compress SQL for display convenience.
        for statement in statements:
            for key, value in statement.items():
                row = {"sql_id": key, "sql": value}
                rows.append(row)
    except xml.etree.ElementTree.ParseError:
        # Remove comment lines.
        text = sqlparse.format(text, strip_comments=True)
        statements = sqlparse.split(text)
        rows = []
        num = 0
        for statement in statements:
            num = num + 1
            row = {"sql_id": num, "sql": statement}
            rows.append(row)
    return rows


def get_base_sqlitem_list(full_sql):
    """Convert full_sql parameter into a SqlItem list.
    :param full_sql: Full SQL string. Each SQL is separated by ";" and does not
        include PLSQL execution blocks or PLSQL object definition blocks.
    :return: List of SqlItem objects.
    """
    list = []
    for statement in sqlparse.split(full_sql):
        statement = sqlparse.format(
            statement, strip_comments=True, reindent=True, keyword_case="lower"
        )
        if len(statement) <= 0:
            continue
        item = SqlItem(statement=statement)
        list.append(item)
    return list


def get_full_sqlitem_list(full_sql, db_name):
    """Get SqlItem list for SQL content, including PLSQL parts.
    :param full_sql: Full SQL content.
    :return: SqlItem list.
    """

    """SQL preprocessing step 1: automatically add PLSQL block end marker.
    Based on PLSQL syntax, detect end markers and append "$$" at block end
    (as a standalone line) so the platform can identify PLSQL block endings.
    Also avoid interference from comments inside PLSQL blocks.
    """
    pattern = r"(;(\s)*\n(/$|/\s))"
    full_sql = re.sub(pattern, ";\n/\n$$", full_sql, flags=re.I)

    """SQL preprocessing step 2: automatically add PLSQL block start marker.
    Based on PLSQL syntax, detect start markers and insert "delimiter $$"
    (as a standalone line) before PLSQL blocks.
    PLSQL blocks include:
    anonymous blocks starting with declare/begin, procedures, functions,
    triggers, packages/package bodies, and type/type bodies.
    """

    """1) Normalize PLSQL block-start statements.
    For start clauses such as "create or replace procedure", collapse potential
    line breaks so the full start marker stays on one line for later processing.
    """
    pattern_dict = {
        r"(create\s+or\s+replace\s+procedure)": "create or replace procedure",
        r"(create\s+or\s+replace\s+function)": "create or replace function",
        r"(create\s+or\s+replace\s+trigger)": "create or replace trigger",
        r"(create\s+or\s+replace\s+package)": "create or replace package",
        r"(create\s+or\s+replace\s+type)": "create or replace type",
        r"(create\s+procedure)": "create procedure",
        r"(create\s+function)": "create function",
        r"(create\s+trigger)": "create trigger",
        r"(create\s+package)": "create package",
        r"(create\s+type)": "create type",
    }

    for pattern in pattern_dict:
        full_sql = re.sub(pattern, pattern_dict[pattern], full_sql, flags=re.I)

    """2) Split SQL text by newline and process line by line.
    Detect PLSQL start markers and prepend "delimiter $$" (standalone line)
    as block-start marker.
    The is_inside_plsqlblock flag prevents false positives for markers that
    may appear inside PLSQL blocks.
    """
    pre_sql_list = full_sql.split("\n")
    full_sql_new = ""
    is_inside_plsqlblock = 0

    # Process SQL text line by line.
    for line in pre_sql_list:
        # If line starts with declare/begin and we are outside a PLSQL block,
        # prepend "delimiter $$" marker (standalone line).
        pattern = r"^(declare|begin)"
        groups = re.match(pattern, line.lstrip(), re.IGNORECASE)
        if groups and is_inside_plsqlblock == 0:
            line = "delimiter $$" + "\n" + line
            # Mark that parser is now inside a PLSQL block.
            is_inside_plsqlblock = 1

        # If line starts with create [or replace] function/procedure/trigger/
        # package/type and we are outside a PLSQL block, prepend marker.
        pattern = (
            r"^create\s+(or\s+replace\s+)?(function|procedure|trigger|package|type)\s"
        )
        groups = re.match(pattern, line.lstrip(), re.IGNORECASE)
        if groups and is_inside_plsqlblock == 0:
            line = "delimiter $$" + "\n" + line
            # Mark that parser is now inside a PLSQL block.
            is_inside_plsqlblock = 1

        # If line equals "$$", mark that parser exits PLSQL block.
        if line.strip() == "$$":
            is_inside_plsqlblock = 0
        full_sql_new = full_sql_new + line + "\n"

    list = []

    # Define start delimiter; parentheses keep delimiter in re.split result.
    regex_delimiter = r"(delimiter\s*\$\$)"
    # package body must appear before package, otherwise it will never match.
    regex_objdefine = r'create\s+or\s+replace\s+(function|procedure|trigger|package\s+body|package|type\s+body|type)\s+("?\w+"?\.)?"?\w+"?[\s+|\(]'
    # Object naming pattern with double quotes on both sides.
    regex_objname = r'^".+"$'

    sql_list = re.split(pattern=regex_delimiter, string=full_sql_new, flags=re.I)

    # delimiter_flag => delimiter marker, 0: no, 1: yes
    # When delimiter marker is seen, this SQL block should be checked for PLSQL.
    # PLSQL existence criterion: block contains "$$".

    delimiter_flag = 0
    for sql in sql_list:
        # Trim leading/trailing spaces and extra whitespace.
        sql = sql.strip()

        # Skip empty strings.
        if len(sql) <= 0:
            continue

        # This line is a delimiter marker; skip it.
        if re.match(regex_delimiter, sql):
            delimiter_flag = 1
            continue

        if delimiter_flag == 1:
            # SQL block after delimiter $$ marker.

            # Find "$$" end marker.
            pos = sql.find("$$")
            length = len(sql)
            if pos > -1:
                # This sqlitem contains "$$" end marker.
                # Process PLSQL block and determine block type first.
                plsql_block = sql[0:pos].strip()
                # Remove trailing "/" from PLSQL block if present.
                while True:
                    if plsql_block[-1:] == "/":
                        plsql_block = plsql_block[:-1].strip()
                    else:
                        break

                search_result = re.search(regex_objdefine, plsql_block, flags=re.I)

                # Keyword search with two cases:
                # Case 1: object-definition execution block
                # Case 2: anonymous execution block

                if search_result:
                    # Keyword found: case 1.

                    str_plsql_match = search_result.group()
                    str_plsql_type = search_result.groups()[0]

                    idx = str_plsql_match.index(str_plsql_type)
                    nm_str = str_plsql_match[idx + len(str_plsql_type) :].strip()

                    if nm_str[-1:] == "(":
                        nm_str = nm_str[:-1]
                    nm_list = nm_str.split(".")

                    if len(nm_list) > 1:
                        # Object name with owner, e.g. object_owner.object_name.

                        # Get object_owner.
                        if re.match(regex_objname, nm_list[0]):
                            # object_owner with double quotes.
                            object_owner = nm_list[0].strip().strip('"')
                        else:
                            # object_owner without double quotes.
                            object_owner = nm_list[0].upper().strip().strip("'")

                        # Get object_name.
                        if re.match(regex_objname, nm_list[1]):
                            # object_name with double quotes.
                            object_name = nm_list[1].strip().strip('"')
                        else:
                            # object_name without double quotes.
                            object_name = nm_list[1].upper().strip()
                    else:
                        # Without owner.
                        object_owner = db_name
                        if re.match(regex_objname, nm_list[0]):
                            # object_name with double quotes.
                            object_name = nm_list[0].strip().strip('"')
                        else:
                            # object_name without double quotes.
                            object_name = nm_list[0].upper().strip()

                    tmp_object_type = str_plsql_type.upper()
                    tmp_stmt_type = "PLSQL"
                    if tmp_object_type == "VIEW":
                        tmp_stmt_type = "SQL"

                    item = SqlItem(
                        statement=plsql_block,
                        stmt_type=tmp_stmt_type,
                        object_owner=object_owner,
                        object_type=tmp_object_type,
                        object_name=object_name,
                    )
                    list.append(item)
                else:
                    # No keyword found: case 2, anonymous executable block.
                    item = SqlItem(
                        statement=plsql_block.strip(),
                        stmt_type="PLSQL",
                        object_owner=db_name,
                        object_type="ANONYMOUS",
                        object_name="ANONYMOUS",
                    )
                    list.append(item)

                if length > pos + 2:
                    # Process statements after "$$" as executable SQL statements.
                    # For create view/sequence/table statements where trailing "/"
                    # was followed by "$$" in preprocessing, remove "/\n$$" first.
                    sql_area = sql[pos + 2 :].replace("/\n$$", "").strip()
                    if len(sql_area) > 0:
                        tmp_list = get_base_sqlitem_list(sql_area)
                        list.extend(tmp_list)

            else:
                # No "$$" marker found, treat as executable SQL statement set.
                tmp_list = get_base_sqlitem_list(sql)
                list.extend(tmp_list)

            # Reset delimiter flag after processing current delimiter block.
            delimiter_flag = 0
        else:
            # Current block is normal SQL ending with ";".
            # Remove "/\n$$" artifact before sending to get_base_sqlitem_list.
            sql = sql.replace("/\n$$", "")
            tmp_list = get_base_sqlitem_list(sql)
            list.extend(tmp_list)
    return list


def get_exec_sqlitem_list(reviewResult, db_name):
    """Generate new SQL list based on review results.
    :param reviewResult: SQL review result list.
    :param db_name:
    :return:
    """
    list = []

    for item in reviewResult:
        list.append(
            SqlItem(
                statement=item["sql"],
                stmt_type=item["stmt_type"],
                object_owner=item["object_owner"],
                object_type=item["object_type"],
                object_name=item["object_name"],
            )
        )
    return list


def filter_db_list(db_list, db_name_regex: str, is_match_regex: bool, key="value"):
    """
    Filter database names according to configured regex rules.

    :param db_list: Database list to filter; may be a list of strings or dicts.
    Sample data:
    1. db_list=[{"value": 0, "text": 0, "value": 1, "text": 1}]
    2. db_list=["a_db","b_db"]
    :param db_name_regex: Configured database regex.
    :param is_match_regex: Whether regex should match (True) or not match (False).
    :param key: If db_list contains dicts, key used for matching. Default is 'value'.
    :return: Filtered database name list or dict list.
    """
    if not db_name_regex:
        return db_list  # Return original db_list when no regex is provided.

    try:
        db_regex = re.compile(db_name_regex)  # Compile regex.
    except re.error:
        raise ValueError(f"Regex parsing error: {db_name_regex}")

    filtered_list = []

    # Process db_list by item type.
    for db in db_list:
        # Determine value to test (string or specific key in dict).
        db_value = str(db[key]) if isinstance(db, dict) else db
        is_match = bool(db_regex.match(db_value))
        # Filter by is_match_regex.
        if (is_match_regex and is_match) or (not is_match_regex and not is_match):
            filtered_list.append(db)
    return filtered_list
