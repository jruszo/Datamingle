# -*- coding: UTF-8 -*-
"""
@author: feiazifeiazi
@license: Apache Licence
@file: xx.py
@time: 2024-08-01
"""

__author__ = "feiazifeiazi"

import logging
import os
import re
import traceback
from opensearchpy import OpenSearch
import simplejson as json
import sqlparse

from common.utils.timer import FuncTimer
from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult
from common.config import SysConfig
import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError

logger = logging.getLogger("default")


class QueryParamsSearch:
    def __init__(
        self,
        index: str = None,
        path: str = None,
        params: str = None,
        method: str = None,
        size: int = 100,
        sql: str = None,
        query_body: dict = None,
    ):
        self.index = index if index is not None else ""
        self.path = path if path is not None else ""
        self.method = method if method is not None else ""
        self.params = params
        self.size = size
        self.sql = sql if sql is not None else ""
        self.query_body = query_body if query_body is not None else {}


class ElasticsearchDocument:
    """ES document object."""

    def __init__(
        self,
        sql: str = None,
        method: str = None,
        index_name: str = None,
        api_endpoint: str = "",
        doc_id: str = None,
        doc_data_body: str = None,
    ):
        self.sql = sql
        self.method = method.upper() if method is not None else None
        self.index_name = index_name
        self.api_endpoint = api_endpoint.lower() if api_endpoint is not None else ""
        self.doc_id = doc_id
        self.doc_data_body = doc_data_body

    def describe(self) -> str:
        """Return a formatted description."""
        return f"[index_name: {self.index_name}, method: {self.method}, api_endpoint: {self.api_endpoint}, doc_id: {self.doc_id}]"


class ElasticsearchEngineBase(EngineBase):
    """
    Base implementation for search engines like Elasticsearch and OpenSearch.
    If behavior differences are small, use if/else in this base class.
    If differences are large, implement in subclasses.
    """

    def __init__(self, instance=None):
        self.conn = None  # type: Elasticsearch  # Explicit type hint.
        self.db_separator = "__"  # Name separator.
        # Restrict supported subclasses.
        self.search_name = ["Elasticsearch", "OpenSearch"]
        if self.name not in self.search_name:
            raise ValueError(
                f"Invalid name: {self.name}. Must be one of {self.search_name}."
            )
        super().__init__(instance=instance)

    def get_connection(self, db_name=None):
        """Return a connection instance."""

    def test_connection(self):
        """Test whether instance connection is valid."""
        return self.get_all_databases()

    name: str = "SearchBase"
    info: str = "SearchBase Engine"

    def get_all_databases(self):
        """Get all "database" names extracted from index names."""
        try:
            self.get_connection()
            # Get all aliases; if none exist, index name itself is used.
            indices = self.conn.indices.get_alias(index=self.db_name)
            database_names = set()
            database_names.add("system")  # Database name used for system tables.
            for index_name in indices.keys():
                if self.db_separator in index_name:
                    db_name = index_name.split(self.db_separator)[0]
                    database_names.add(db_name)
            database_names.add("other")  # Database name used when no separator exists.
            database_names_sorted = sorted(database_names)
            return ResultSet(rows=database_names_sorted)
        except Exception as e:
            logger.error(f"Error getting databases: {e}{traceback.format_exc()}")
            raise Exception(f"Error getting databases: {str(e)}")

    def get_all_tables(self, db_name, **kwargs):
        """Get all tables related to the given database name.

        Table names that start with a dot are hidden system tables.
        """
        try:
            self.get_connection()
            indices = self.conn.indices.get_alias(index=self.db_name)
            tables = set()

            db_mapping = {
                "system": "",
                "other": "",
            }
            # Handle database names split by separator.
            if db_name not in db_mapping:
                index_prefix = db_name.rstrip(self.db_separator) + self.db_separator
                tables = [
                    index for index in indices.keys() if index.startswith(index_prefix)
                ]
            else:
                # Handle system and other.
                if db_name == "system":
                    # Add system APIs as pseudo table names.
                    tables.add("/_cat/indices/" + self.db_name)
                    tables.add("/_cat/nodes")
                    tables.add("/_security/role")
                    tables.add("/_security/user")

                for index_name in indices.keys():
                    if index_name.startswith("."):
                        # if db_name == "system":
                        #     tables.add(index_name)
                        continue
                    elif index_name.startswith(db_name):
                        tables.add(index_name)
                        if db_name == "system":
                            tables.add("/_cat/indices/" + db_name)
                        continue
                    elif self.db_separator in index_name:
                        separator_db_name = index_name.split(self.db_separator)[0]
                        if db_name == "system":
                            tables.add("/_cat/indices/" + separator_db_name)
                    else:
                        if db_name == "other":
                            tables.add(index_name)
            tables_sorted = sorted(tables)
            return ResultSet(rows=tables_sorted)
        except Exception as e:
            raise Exception(f"Error getting table list: {str(e)}")

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all columns."""
        result_set = ResultSet(full_sql=f"{tb_name}/_mapping")
        if tb_name.startswith(("/", "_")):
            return result_set
        else:
            try:
                self.get_connection()
                mapping = self.conn.indices.get_mapping(index=tb_name)
                properties = (
                    mapping.get(tb_name, {}).get("mappings", {}).get("properties", None)
                )
                # Return field names.
                result_set.column_list = ["column_name"]
                if properties is None:
                    result_set.rows = ["None"]
                else:
                    result_set.rows = list(properties.keys())
                return result_set
            except Exception as e:
                raise Exception(f"Error getting fields: {str(e)}")

    def describe_table(self, db_name, tb_name, **kwargs):
        """Table structure."""
        result_set = ResultSet(full_sql=f"{tb_name}/_mapping")
        if tb_name.startswith(("/", "_")):
            return result_set
        else:
            try:
                self.get_connection()
                mapping = self.conn.indices.get_mapping(index=tb_name)
                properties = (
                    mapping.get(tb_name, {}).get("mappings", {}).get("properties", None)
                )
                # Build list structure with column name, type, and other info.
                result_set.column_list = ["column_name", "type", "fields"]
                if properties is None:
                    result_set.rows = [("None", "None", "None")]
                else:
                    result_set.rows = [
                        (
                            column,
                            details.get("type"),
                            json.dumps(details.get("fields", {})),
                        )
                        for column, details in properties.items()
                    ]
                return result_set
            except Exception as e:
                raise Exception(f"Error getting fields: {str(e)}")

    def query_check(self, db_name=None, sql=""):
        """Statement validation."""
        result = {
            "msg": "Statement check passed.",
            "bad_query": False,
            "filtered_sql": sql,
            "has_star": False,
        }
        sql = sql.rstrip(";").strip()
        result["filtered_sql"] = sql
        # Validate statement starts with 'get' or 'select'.
        if re.match(r"^get", sql, re.I):
            pass
        elif re.match(r"^select", sql, re.I):
            try:
                sql = sqlparse.format(sql, strip_comments=True)
                sql = sqlparse.split(sql)[0]
                result["filtered_sql"] = sql.strip()
            except IndexError:
                result["bad_query"] = True
                result["msg"] = "No valid SQL statement found."
        else:
            result["msg"] = (
                "Statement check failed: statement must start with 'get' or "
                "'select'. Example: GET /dmp__iv/_search or "
                "select * from dmp__iv limit 10;"
            )
            result["bad_query"] = True
        return result

    def filter_sql(self, sql="", limit_num=0):
        """Filter SQL statement.

        Add or rewrite query LIMIT to enforce row limits.
        SQL-limit logic is based on MySQL implementation.
        """
        #
        sql = sql.rstrip(";").strip()
        if re.match(r"^get", sql, re.I):
            pass
        elif re.match(r"^select", sql, re.I):
            # LIMIT N
            limit_n = re.compile(r"limit\s+(\d+)\s*$", re.I)
            # LIMIT M OFFSET N
            limit_offset = re.compile(r"limit\s+(\d+)\s+offset\s+(\d+)\s*$", re.I)
            # LIMIT M,N
            offset_comma_limit = re.compile(r"limit\s+(\d+)\s*,\s*(\d+)\s*$", re.I)
            if limit_n.search(sql):
                sql_limit = limit_n.search(sql).group(1)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_n.sub(f"limit {limit_num};", sql)
            elif limit_offset.search(sql):
                sql_limit = limit_offset.search(sql).group(1)
                sql_offset = limit_offset.search(sql).group(2)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_offset.sub(f"limit {limit_num} offset {sql_offset};", sql)
            elif offset_comma_limit.search(sql):
                sql_offset = offset_comma_limit.search(sql).group(1)
                sql_limit = offset_comma_limit.search(sql).group(2)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = offset_comma_limit.sub(f"limit {sql_offset},{limit_num};", sql)
            else:
                sql = f"{sql} limit {limit_num};"
        else:
            sql = f"{sql};"
        return sql

    def query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters=None,
        **kwargs,
    ):
        """Execute query."""
        try:
            result_set = ResultSet(full_sql=sql)

            # Parse query string.
            query_params = self.parse_es_select_query_to_query_params(sql, limit_num)
            self.get_connection()
            # Admin/query-management endpoints.
            if query_params.path.startswith("/_cat/indices"):
                # Add "v" to show column headers. OpenSearch expects string true.
                if "v" not in query_params.params:
                    query_params.params["v"] = "true"
                response = self.conn.cat.indices(
                    index=query_params.index, params=query_params.params
                )
                response_body = ""
                if isinstance(response, str):
                    response_body = response
                else:
                    response_body = response.body
                response_data = self.parse_cat_indices_response(response_body)
                # Set column names when data exists.
                if response_data:
                    result_set.column_list = list(response_data[0].keys())
                    result_set.rows = [tuple(row.values()) for row in response_data]
                else:
                    result_set.column_list = []
                    result_set.rows = []
                    result_set.affected_rows = 0
            elif query_params.path.startswith("/_security/role"):
                result_set = self._security_role(sql, query_params)
            elif query_params.path.startswith("/_security/user"):
                result_set = self._security_user(sql, query_params)
            elif query_params.sql and self.name == "Elasticsearch":
                query_body = {"query": query_params.sql}
                response = self.conn.sql.query(body=query_body)
                # Extract columns and rows.
                columns = response.get("columns", [])
                rows = response.get("rows", [])
                # Use field names as column headers.
                column_list = [col["name"] for col in columns]

                # Convert list/dict values to JSON strings.
                formatted_rows = []
                for row in rows:
                    formatted_row = []
                    for col_name, value in zip(column_list, row):
                        # Convert list/dict fields to JSON strings.
                        if isinstance(value, (list, dict)):
                            formatted_row.append(json.dumps(value))
                        else:
                            formatted_row.append(value)
                    formatted_rows.append(formatted_row)
                # Build result set.
                result_set.rows = formatted_rows
                result_set.column_list = column_list
            elif query_params.sql and self.name == "OpenSearch":
                query_body = {"query": query_params.sql}
                response = self.conn.transport.perform_request(
                    method="POST", url="/_opendistro/_sql", body=query_body
                )
                # Extract columns and rows.
                columns = response.get("schema", [])
                rows = response.get("datarows", [])
                # Use field names as column headers.
                column_list = [col["name"] for col in columns]

                # Convert list/dict values to JSON strings.
                formatted_rows = []
                for row in rows:
                    formatted_row = []
                    for col_name, value in zip(column_list, row):
                        # Convert list/dict fields to JSON strings.
                        if isinstance(value, (list, dict)):
                            formatted_row.append(json.dumps(value))
                        else:
                            formatted_row.append(value)
                    formatted_rows.append(formatted_row)
                # Build result set.
                result_set.rows = formatted_rows
                result_set.column_list = column_list
            else:
                # Execute search query.
                response = self.conn.search(
                    index=query_params.index,
                    body=query_params.query_body,
                    params=query_params.params,
                )

                # Extract search hits.
                hits = response.get("hits", {}).get("hits", [])
                # Convert list/dict values to JSON strings.
                rows = []
                all_search_keys = {}  # Collect all field names.
                all_search_keys["_id"] = None
                for hit in hits:
                    # Get document ID and source payload.
                    doc_id = hit.get("_id")
                    source_data = hit.get("_source", {})

                    # Convert list/dict fields to JSON strings.
                    for key, value in source_data.items():
                        all_search_keys[key] = None
                        if isinstance(value, (list, dict)):
                            source_data[key] = json.dumps(value)

                    # Build result row.
                    row = {"_id": doc_id, **source_data}
                    rows.append(row)

                column_list = list(all_search_keys.keys())
                # Build result set.
                result_set.rows = []
                for row in rows:
                    # Fill each row in column_list order.
                    result_row = tuple(row.get(key, None) for key in column_list)
                    result_set.rows.append(result_row)
                result_set.column_list = column_list
            result_set.affected_rows = len(result_set.rows)
            return result_set
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")

    def _security_role(self, sql, query_params: QueryParamsSearch):
        """Role query method. Implement in subclass."""

    def _security_user(self, sql, query_params: QueryParamsSearch):
        """User query method. Implement in subclass."""

    def parse_cat_indices_response(self, response_text):
        """Parse cat indices response."""
        # Split response text into lines.
        lines = response_text.strip().splitlines()
        # Read header columns.
        headers = lines[0].strip().split()
        # Parse each row.
        indices_info = []
        for line in lines[1:]:
            # Split by spaces and map to headers.
            values = line.strip().split(maxsplit=len(headers) - 1)
            index_info = dict(zip(headers, values))
            indices_info.append(index_info)
        return indices_info

    def parse_es_select_query_to_query_params(
        self, search_query_str: str, limit_num: int
    ) -> QueryParamsSearch:
        """Parse search query string into QueryParamsSearch."""

        query_params = QueryParamsSearch()
        sql = search_query_str.rstrip(";").strip()
        if re.match(r"^get", sql, re.I):
            # Parse query string.
            lines = sql.splitlines()
            method_line = lines[0].strip()

            query_body = "\n".join(lines[1:]).strip()
            # Use default query body when empty.
            if not query_body:
                query_body = json.dumps({"query": {"match_all": {}}})

            # Ensure query_body is valid JSON.
            try:
                json_body = json.loads(query_body)
            except json.JSONDecodeError as json_err:
                raise ValueError(
                    f"Cannot parse JSON format. {json_err}. query_body: {query_body}."
                )

            # Extract method and path.
            method, path_with_params = method_line.split(maxsplit=1)
            # Ensure path starts with '/'.
            if not path_with_params.startswith("/"):
                path_with_params = "/" + path_with_params

            # Split path and query params.
            path, params_str = (
                path_with_params.split("?", 1)
                if "?" in path_with_params
                else (path_with_params, "")
            )
            params = {}
            if params_str:
                for pair in params_str.split("&"):
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                    else:
                        key = pair
                        value = ""
                    params[key] = value
            index_pattern = ""
            # Determine path type and extract index pattern.
            if path.startswith("/_cat/indices"):
                # _cat API path.
                path_parts = path.split("/")
                if len(path_parts) > 3:
                    index_pattern = path_parts[3]
                if not index_pattern:
                    index_pattern = "*"
            elif path.startswith("/_security/role"):
                path_parts = path.split("/")
                index_pattern = "*"
            elif path.startswith("/_security/user"):
                path_parts = path.split("/")
                index_pattern = "*"
            elif "/_search" in path:
                # Default case: normal index path.
                path_parts = path.split("/")
                if len(path_parts) > 1:
                    index_pattern = path_parts[1]

            if not index_pattern:
                raise Exception("Index name not found.")

            size = limit_num if limit_num > 0 else 100
            # Set size when not present in query JSON.
            if "size" not in json_body:
                json_body["size"] = size
            # Build QueryParams object.
            query_params = QueryParamsSearch(
                index=index_pattern,
                path=path_with_params,
                params=params,
                method=method,
                size=size,
                query_body=json_body,
            )
        elif re.match(r"^select", sql, re.I):
            query_params = QueryParamsSearch(sql=sql)
        return query_params

    def execute_check(self, db_name=None, sql=""):
        """Pre-execution validation for workflow statements.

        Rules:
        - PUT with index and no API endpoint means index creation.
        - PUT with _doc must include document ID.
        - POST with index and no endpoint is invalid.
        - POST with _doc supports with or without ID.
        - _search is query-only and not allowed in execution workflow.
        - DELETE without _doc is equivalent to dropping index, which is disallowed.
        - DELETE with _doc must include document ID.
        - _update, _update_by_query, _delete_by_query must use POST.
        """
        check_result = ReviewSet(full_sql=sql)
        rowid = 1
        documents = self.__split_sql(sql)
        for doc in documents:
            is_pass = False
            doc_desc = doc.describe()
            if re.match(r"^get|^select", doc.sql, re.I):
                result = ReviewResult(
                    id=rowid,
                    errlevel=2,
                    stagestatus="Rejected: unsupported statement",
                    errormessage=(
                        "Only API methods like PUT/POST/DELETE are supported. "
                        "For GET/SELECT queries, use SQL query feature."
                    ),
                    sql=doc.sql,
                )
            elif re.match(r"^#", doc.sql, re.I):
                result = ReviewResult(
                    id=rowid,
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="This is a comment line.",
                    sql=doc.sql,
                    affected_rows=0,
                    execute_time=0,
                )
            elif not doc.index_name:
                result = ReviewResult(
                    id=rowid,
                    errlevel=2,
                    stagestatus="Rejected: unsupported statement",
                    errormessage=(
                        f"Request must include index name or be parseable. Parsed: {doc_desc}"
                    ),
                    sql=doc.sql,
                )
            elif doc.method == "DELETE":
                if not doc.doc_id:
                    result = ReviewResult(
                            id=rowid,
                            errlevel=2,
                            stagestatus="Rejected: unsupported statement",
                            errormessage="DELETE operation must include document ID.",
                            sql=doc.sql,
                        )
                else:
                    if is_pass == False:
                        is_pass = True
            elif not doc.api_endpoint:
                if doc.method == "PUT":
                    if not doc.doc_data_body or (
                        "mappings" in doc.doc_data_body
                        or "settings" in doc.doc_data_body
                    ):
                        result = ReviewResult(
                            id=rowid,
                            errlevel=0,
                            stagestatus="Audit completed",
                            errormessage=f"Audit passed. Parsed result: create index [index_name: {doc.index_name}]",
                            sql=doc.sql,
                        )
                    else:
                        result = ReviewResult(
                            id=rowid,
                            errlevel=2,
                            stagestatus="Rejected: unsupported statement",
                            errormessage=(
                                "For PUT index creation, request body can be empty "
                                "or include mappings/settings."
                            ),
                            sql=doc.sql,
                        )
                elif doc.method == "POST":
                    result = ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Rejected: unsupported statement",
                        errormessage=(
                            f"POST request must specify API endpoint, e.g. _doc. Parsed: {doc_desc}"
                        ),
                        sql=doc.sql,
                    )
                else:
                    result = ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Rejected: unsupported statement",
                        errormessage=f"Unsupported operation. Parsed: {doc_desc}",
                        sql=doc.sql,
                        affected_rows=0,
                        execute_time=0,
                    )
            elif doc.api_endpoint == "_doc":
                if doc.method == "PUT":
                    if not doc.doc_id:
                        result = ReviewResult(
                            id=rowid,
                            errlevel=2,
                            stagestatus="Rejected: unsupported statement",
                            errormessage="PUT request must include document ID.",
                            sql=doc.sql,
                        )
                    else:
                        if is_pass == False:
                            is_pass = True
                elif doc.method == "POST":
                    if is_pass == False:
                        is_pass = True
                else:
                    result = ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Rejected: unsupported statement",
                        errormessage=f"Unsupported operation. Parsed: {doc_desc}",
                        sql=doc.sql,
                        affected_rows=0,
                        execute_time=0,
                    )
            elif doc.api_endpoint == "_search":
                result = ReviewResult(
                    id=rowid,
                    errlevel=2,
                    stagestatus="Rejected: unsupported statement",
                    errormessage="_search is a query-only method.",
                    sql=doc.sql,
                )
            elif doc.api_endpoint == "_update":
                if doc.method == "POST":
                    if not doc.doc_id:
                        result = ReviewResult(
                            id=rowid,
                            errlevel=2,
                            stagestatus="Rejected: unsupported statement",
                            errormessage=f"POST {doc.api_endpoint} must include document ID.",
                            sql=doc.sql,
                        )
                    else:
                        if is_pass == False:
                            is_pass = True
                else:
                    result = ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Rejected: unsupported statement",
                        errormessage=(
                            f"Unsupported operation: {doc.api_endpoint} must use POST. Parsed: {doc_desc}"
                        ),
                        sql=doc.sql,
                        affected_rows=0,
                        execute_time=0,
                    )
            elif doc.api_endpoint == "_update_by_query":
                if doc.method == "POST":
                    if is_pass == False:
                        is_pass = True
                else:
                    result = ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Rejected: unsupported statement",
                        errormessage=(
                            f"Unsupported operation: {doc.api_endpoint} must use POST. Parsed: {doc_desc}"
                        ),
                        sql=doc.sql,
                        affected_rows=0,
                        execute_time=0,
                    )
            elif doc.api_endpoint == "_delete_by_query":
                if doc.method == "POST":
                    if is_pass == False:
                        is_pass = True
                else:
                    result = ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Rejected: unsupported statement",
                        errormessage=(
                            f"Unsupported operation: {doc.api_endpoint} must use POST. Parsed: {doc_desc}"
                        ),
                        sql=doc.sql,
                        affected_rows=0,
                        execute_time=0,
                    )
            elif doc.api_endpoint not in [
                "",
                "_doc",
                "_update_by_query",
                "_update",
                "_delete_by_query",
            ]:
                result = ReviewResult(
                    id=rowid,
                    errlevel=2,
                    stagestatus="Rejected: unsupported statement",
                    errormessage=(
                        "Supported API endpoints are: empty, _doc, _update, "
                        "_update_by_query, _delete_by_query."
                    ),
                    sql=doc.sql,
                )
            else:
                result = ReviewResult(
                    id=rowid,
                    errlevel=2,
                    stagestatus="Rejected: unsupported statement",
                    errormessage=f"Unsupported operation. Parsed: {doc_desc}",
                    sql=doc.sql,
                    affected_rows=0,
                    execute_time=0,
                )
            # Generic success case.
            if is_pass:
                result = ReviewResult(
                    id=rowid,
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage=f"Audit passed. Parsed result: {doc_desc}",
                    sql=doc.sql,
                    affected_rows=0,
                    execute_time=0,
                )

            check_result.rows.append(result)
            rowid += 1
        # Count warnings and errors.
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1
        return check_result

    def execute_workflow(self, workflow):
        """Execute workflow and return ReviewSet."""
        sql = workflow.sqlworkflowcontent.sql_content
        docs = self.__split_sql(sql)
        execute_result = ReviewSet(full_sql=sql)
        line = 0
        try:
            conn = self.get_connection(db_name=workflow.db_name)
            for doc in docs:
                line += 1
                if re.match(r"^#", doc.sql, re.I):
                    execute_result.rows.append(
                        ReviewResult(
                            id=line,
                            errlevel=0,
                            stagestatus="Execute Successfully",
                            errormessage="Comment line does not need execution.",
                            sql=doc.sql,
                            affected_rows=0,
                            execute_time=0,
                        )
                    )
                elif doc.method == "DELETE":
                    reviewResult = self.__delete_data(conn, doc)
                    reviewResult.id = line
                    execute_result.rows.append(reviewResult)
                elif doc.api_endpoint == "":
                    # Create index.
                    reviewResult = self.__create_index(conn, doc)
                    reviewResult.id = line
                    execute_result.rows.append(reviewResult)
                elif doc.api_endpoint == "_update":
                    reviewResult = self.__update(conn, doc)
                    reviewResult.id = line
                    execute_result.rows.append(reviewResult)
                elif doc.api_endpoint == "_update_by_query":
                    reviewResult = self.__update_by_query(conn, doc)
                    reviewResult.id = line
                    execute_result.rows.append(reviewResult)
                elif doc.api_endpoint == "_delete_by_query":
                    reviewResult = self.__delete_by_query(conn, doc)
                    reviewResult.id = line
                    execute_result.rows.append(reviewResult)
                elif doc.api_endpoint == "_doc":
                    reviewResult = self.__add_or_update(conn, doc)
                    reviewResult.id = line
                    execute_result.rows.append(reviewResult)
                else:
                    raise Exception(f"Unsupported API type: {doc.api_endpoint}")
        except Exception as e:
            logger.warning(
                f"ES command execution failed, sql: {doc.sql}, error: {traceback.format_exc()}"
            )
            # Append current error statement to execution result.
            execute_result.error = str(e)
            execute_result.rows.append(
                ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage=f"Error message: {e}",
                    sql=doc.sql,
                    affected_rows=0,
                    execute_time=0,
                )
            )
        if execute_result.error:
            # If failed, append remaining statements as skipped.
            for doc in docs[line:]:
                line += 1
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Audit completed",
                        errormessage="Previous statement failed; not executed.",
                        sql=doc.sql,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
        return execute_result

    def __update(self, conn, doc):
        """ES update method."""
        errlevel = 0
        with FuncTimer() as t:
            try:
                response = conn.update(
                    index=doc.index_name,
                    id=doc.doc_id,
                    body=doc.doc_data_body,
                )
                successful_count = response.get("_shards", {}).get("successful", None)
                response_str = str(response)
            except Exception as e:
                error_message = str(e)
                if "NotFoundError" in error_message:
                    response_str = "document missing: " + error_message
                    successful_count = 0
                    errlevel = 1
                else:
                    raise
        return ReviewResult(
            errlevel=errlevel,
            stagestatus="Execute Successfully",
            errormessage=response_str,
            sql=doc.sql,
            affected_rows=successful_count,
            execute_time=t.cost,
        )

    def __add_or_update(self, conn, doc):
        """ES add_or_update method."""
        with FuncTimer() as t:
            if doc.api_endpoint == "_doc":
                response = conn.index(
                    index=doc.index_name,
                    id=doc.doc_id,
                    body=doc.doc_data_body,
                )
            else:
                raise Exception(f"Unsupported API type: {doc.api_endpoint}")

            successful_count = response.get("_shards", {}).get("successful", None)
            response_str = str(response)
        return ReviewResult(
            errlevel=0,
            stagestatus="Execute Successfully",
            errormessage=response_str,
            sql=doc.sql,
            affected_rows=successful_count,
            execute_time=t.cost,
        )

    def __update_by_query(self, conn, doc):
        """ES update_by_query method."""
        errlevel = 0
        with FuncTimer() as t:
            try:
                response = conn.update_by_query(
                    index=doc.index_name, body=doc.doc_data_body
                )
                successful_count = response.get("total", 0)
                response_str = str(response)
            except Exception as e:
                raise e
        return ReviewResult(
            errlevel=errlevel,
            stagestatus="Execute Successfully",
            errormessage=response_str,
            sql=doc.sql,
            affected_rows=successful_count,
            execute_time=t.cost,
        )

    def __delete_by_query(self, conn, doc):
        """ES delete_by_query method."""
        errlevel = 0
        with FuncTimer() as t:
            try:
                response = conn.delete_by_query(
                    index=doc.index_name,
                    body=doc.doc_data_body,
                    params={"scroll_size": "3000", "slices": "2"},
                )
                successful_count = response.get("total", 0)
                response_str = str(response)
            except Exception as e:
                raise e
        return ReviewResult(
            errlevel=errlevel,
            stagestatus="Execute Successfully",
            errormessage=response_str,
            sql=doc.sql,
            affected_rows=successful_count,
            execute_time=t.cost,
        )

    def __create_index(self, conn, doc):
        """ES index creation method."""
        errlevel = 0
        with FuncTimer() as t:
            try:
                response = conn.indices.create(
                    index=doc.index_name, body=doc.doc_data_body
                )
                successful_count = 0
                response_str = str(response)
            except Exception as e:
                error_message = str(e)
                if "already_exists" in error_message:
                    response_str = "index already exists: " + error_message
                    successful_count = 0
                    errlevel = 1
                else:
                    raise

        return ReviewResult(
            errlevel=errlevel,
            stagestatus="Execute Successfully",
            errormessage=response_str,
            sql=doc.sql,
            affected_rows=successful_count,
            execute_time=t.cost,
        )

    def __delete_data(self, conn, doc):
        """
        Delete data.
        """
        errlevel = 0
        if not doc.doc_id:
            response_str = "DELETE operation must include document ID."
            successful_count = 0
        with FuncTimer() as t:
            try:
                response = conn.delete(index=doc.index_name, id=doc.doc_id)
                successful_count = response.get("_shards", {}).get("successful", None)
                response_str = str(response)
            except Exception as e:
                error_message = str(e)
                if "NotFoundError" in error_message:
                    response_str = "Document not found: " + error_message
                    successful_count = 0
                    errlevel = 1
                else:
                    raise
        return ReviewResult(
            errlevel=errlevel,
            stagestatus="Execute Successfully",
            errormessage=response_str,
            sql=doc.sql,
            affected_rows=successful_count,
            execute_time=t.cost,
        )

    def __get_document_from_sql(self, sql):
        """
        Parse SQL input and extract index, document ID, and body.
        Return an ElasticsearchDocument instance.
        """
        result = ElasticsearchDocument(sql=sql)
        if re.match(r"^POST |^PUT |^DELETE ", sql, re.I):

            # Extract method and path.
            method, path_with_params = sql.split(maxsplit=1)
            if path_with_params.startswith("{"):
                # Starts with '{' means path part is missing.
                return result
            # Ensure path starts with '/'.
            if not path_with_params.startswith("/"):
                path_with_params = "/" + path_with_params

            parts = path_with_params.split(maxsplit=1)
            path = parts[0]  # Path part.
            doc_data_body = parts[1].strip() if len(parts) > 1 else None

            path_parts = path.split("/")
            # Extract path parts.
            index_name = path_parts[1] if len(path_parts) > 1 else None
            api_endpoint = path_parts[2] if len(path_parts) > 2 else None
            doc_id = path_parts[3] if len(path_parts) > 3 else None
            doc_data_json = None
            if doc_data_body:
                try:
                    doc_data_json = json.loads(doc_data_body)
                except json.JSONDecodeError as json_err:
                    raise ValueError(
                        f"Cannot parse JSON format. {json_err}. doc_data_body: {doc_data_body}."
                    )
            result = ElasticsearchDocument(
                sql=sql,
                method=method,
                index_name=index_name,
                api_endpoint=api_endpoint,
                doc_id=doc_id,
                doc_data_body=doc_data_json,
            )
        return result

    def __split_sql(self, sql):
        """
        Parse multi-line command string into independent commands.
        Convert parsed commands into document objects.
        """
        lines = sql.strip().splitlines()
        commands = []
        current_command = []
        brace_level = 0

        for line in lines:
            stripped_line = line.strip()

            if not stripped_line:
                continue
            if stripped_line.startswith("#"):
                continue

            brace_level += stripped_line.count("{")
            brace_level -= stripped_line.count("}")

            # Append current line to current command.
            current_command.append(stripped_line)

            if brace_level == 0 and current_command:
                commands.append(os.linesep.join(current_command))
                current_command = []

        merged_commands = []
        for command in commands:
            # Merge command to previous one when it starts with '{'.
            if command.startswith("{") and merged_commands:
                merged_commands[-1] += os.linesep + command
            else:
                merged_commands.append(command)

        # Build ElasticsearchDocument list.
        documents = []
        for command in merged_commands:
            doc = self.__get_document_from_sql(command)
            if doc:
                documents.append(doc)
        return documents


class ElasticsearchEngine(ElasticsearchEngineBase):
    """Elasticsearch engine implementation."""

    def __init__(self, instance=None):
        super().__init__(instance=instance)

    name: str = "Elasticsearch"
    info: str = "Elasticsearch Engine"

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn
        if self.instance:
            scheme = "https" if self.instance.is_ssl else "http"
            hosts = [
                {
                    "host": self.host,
                    "port": self.port,
                    "scheme": scheme,
                    "use_ssl": self.instance.is_ssl,
                }
            ]
            http_auth = (
                (self.user, self.password) if self.user and self.password else None
            )
            self.db_name = (self.db_name or "") + "*"
            try:
                # Create Elasticsearch connection. New versions support basic_auth.
                self.conn = Elasticsearch(
                    hosts=hosts,
                    http_auth=http_auth,
                    verify_certs=self.instance.verify_ssl,  # Enable certificate verification.
                )
            except Exception as e:
                raise Exception(f"Failed to establish Elasticsearch connection: {str(e)}")
        if not self.conn:
            raise Exception("Unable to establish Elasticsearch connection.")
        return self.conn

    def _security_role(self, sql, query_params: QueryParamsSearch):
        """TODO role query method."""
        raise NotImplementedError("This method is not implemented yet.")

    def _security_user(self, sql, query_params: QueryParamsSearch):
        """TODO user query method."""
        raise NotImplementedError("This method is not implemented yet.")


class OpenSearchEngine(ElasticsearchEngineBase):
    """OpenSearch engine implementation."""

    def __init__(self, instance=None):
        self.conn = None  # type: OpenSearch  # Explicit type hint.
        super().__init__(instance=instance)

    name: str = "OpenSearch"
    info: str = "OpenSearch Engine"

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn
        if self.instance:
            scheme = "https" if self.instance.is_ssl else "http"
            hosts = [
                {
                    "host": self.host,
                    "port": self.port,
                    "scheme": scheme,
                    "use_ssl": self.instance.is_ssl,
                }
            ]
            http_auth = (
                (self.user, self.password) if self.user and self.password else None
            )
            self.db_name = (self.db_name or "") + "*"

            try:
                # Create OpenSearch connection.
                self.conn = OpenSearch(
                    hosts=hosts,
                    http_auth=http_auth,
                    verify_certs=self.instance.verify_ssl,  # Enable certificate verification.
                )
            except Exception as e:
                raise Exception(f"Failed to establish OpenSearch connection: {str(e)}")
        if not self.conn:
            raise Exception("Unable to establish OpenSearch connection.")
        return self.conn

    def _security_role(self, sql, query_params: QueryParamsSearch):
        """Role query method."""
        result_set = ResultSet(full_sql=sql)
        url = "/_opendistro/_security/api/roles"
        try:
            body = {}
            # "/_security/role"
            response = self.conn.transport.perform_request("GET", url, body=body)
            response_body = response
            if response and isinstance(response_body, (dict)):
                # Use first role object to build dynamic column list.
                first_role_info = next(iter(response.values()), {})
                column_list = ["role_name"] + list(first_role_info.keys())
                formatted_rows = []

                for role_name, role_info in response.items():
                    row = [role_name]
                    for column in first_role_info.keys():
                        value = role_info.get(column, None)
                        # Convert list/dict values to JSON strings.
                        if isinstance(value, (list, dict)):
                            row.append(json.dumps(value))
                        else:
                            row.append(value)
                    formatted_rows.append(row)
                result_set.rows = formatted_rows
                result_set.column_list = column_list
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")
        return result_set

    def _security_user(self, sql, query_params: QueryParamsSearch):
        """User query method."""
        result_set = ResultSet(full_sql=sql)
        url = "/_opendistro/_security/api/user"
        try:
            body = {}
            # "/_security/role"
            response = self.conn.transport.perform_request("GET", url, body=body)
            response_body = response
            if response and isinstance(response_body, (dict)):
                # Use first role object to build dynamic column list.
                first_role_info = next(iter(response.values()), {})
                column_list = ["user_name"] + list(first_role_info.keys())
                formatted_rows = []

                for role_name, role_info in response.items():
                    row = [role_name]
                    for column in first_role_info.keys():
                        value = role_info.get(column, None)
                        # Convert list/dict values to JSON strings.
                        if isinstance(value, (list, dict)):
                            row.append(json.dumps(value))
                        else:
                            row.append(value)
                    formatted_rows.append(row)
                result_set.rows = formatted_rows
                result_set.column_list = column_list
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")
        return result_set
