# -*- coding: UTF-8 -*-
import logging
import os
import tempfile
import csv
import hashlib
import shutil
import datetime
import xml.etree.ElementTree as ET
import sqlparse
import time

import simplejson as json
import pandas as pd
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse, FileResponse
from django.shortcuts import get_object_or_404


from sql.models import SqlWorkflow, AuditEntry
from sql.engines import EngineBase
from sql.engines.models import ReviewSet, ReviewResult
from sql.storage import DynamicStorage
from sql.engines import get_engine
from sql.utils.sql_review import can_view
from common.config import SysConfig

logger = logging.getLogger("default")
EXPORT_FORMATS = {"csv", "tsv", "sql", "xlsx", "json", "xml"}


class OffLineDownLoad(EngineBase):
    """
    Offline download class for executing offline export operations.
    """

    def execute_offline_download(self, workflow):
        """
        Execute offline download operation.
        :param workflow: Workflow instance
        :return: Download result
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Get system configuration.
            config = SysConfig()
            # Validate max_execution_time configuration and fallback to default 60.
            max_execution_time_str = config.get("max_export_rows", "60")
            max_execution_time = (
                int(max_execution_time_str) if max_execution_time_str else 60
            )
            # Get submitted SQL and related workflow information.
            full_sql = workflow.sqlworkflowcontent.sql_content
            full_sql = sqlparse.format(full_sql, strip_comments=True)
            full_sql = sqlparse.split(full_sql)[0]
            sql = full_sql.strip()
            instance = workflow.instance
            schema_name = getattr(workflow, "schema_name", None) or None
            execute_result = ReviewSet(full_sql=sql)
            check_engine = get_engine(instance=instance)

            start_time = time.time()

            try:
                # Execute SQL query.
                storage = DynamicStorage()
                results = check_engine.query(
                    db_name=workflow.db_name,
                    sql=sql,
                    schema_name=schema_name,
                    max_execution_time=max_execution_time * 1000,
                )
                if results.error:
                    raise Exception(results.error)
                if results:
                    columns = results.column_list
                    result = results.rows
                    actual_rows = results.affected_rows

                # Save query result into the requested export artifact.
                get_format_type = workflow.export_format
                file_name = save_to_format_file(
                    get_format_type, result, workflow, columns, temp_dir
                )

                # Save exported file to storage backend.
                tmp_file = os.path.join(temp_dir, file_name)
                with open(tmp_file, "rb") as f:
                    storage.save(file_name, f)

                end_time = time.time()  # Record end time.
                elapsed_time = round(end_time - start_time, 3)
                execute_result.rows = [
                    ReviewResult(
                        stage="Executed",
                        errlevel=0,
                        stagestatus="Execution succeeded",
                        errormessage=f"Saved file: {file_name}",
                        sql=full_sql,
                        execute_time=elapsed_time,
                        affected_rows=actual_rows,
                    )
                ]

                change_workflow = SqlWorkflow.objects.get(id=workflow.id)
                change_workflow.file_name = file_name
                change_workflow.save()

                return execute_result
            except Exception as e:
                # Return failed execution state and error details.
                execute_result.rows = [
                    ReviewResult(
                        stage="Execute failed",
                        error=1,
                        errlevel=2,
                        stagestatus="Aborted",
                        errormessage=f"{e}",
                        sql=full_sql,
                    )
                ]
                execute_result.error = e
                return execute_result
            finally:
                # Close storage connection (mainly required for SFTP after save).
                storage.close()
                # Clean local files and temporary directory.
                shutil.rmtree(temp_dir)

    def pre_count_check(self, workflow):
        """
        Backend checks before workflow submission:
        validate row count threshold and allowed query statements.
        :param workflow: Workflow instance
        :return: Validation result
        """
        # Get system configuration.
        config = SysConfig()
        # Get submitted SQL and related workflow information.
        full_sql = workflow.sql_content
        full_sql = sqlparse.format(full_sql, strip_comments=True)
        full_sql = sqlparse.split(full_sql)[0]
        sql = full_sql.strip()
        count_sql = f"SELECT COUNT(*) FROM ({sql.rstrip(';')}) t"
        clean_sql = sql.strip().lower()
        instance = workflow
        schema_name = getattr(workflow, "schema_name", None) or None
        check_result = ReviewSet(full_sql=sql)
        check_result.syntax_type = 3
        check_engine = get_engine(instance=instance)
        result_set = check_engine.query(
            db_name=workflow.db_name,
            sql=count_sql,
            schema_name=schema_name,
        )
        actual_rows_check = result_set.rows[0][0]
        max_export_rows_str = config.get("max_export_rows", "10000")
        max_export_rows = int(max_export_rows_str) if max_export_rows_str else 10000

        allowed_prefixes = ("select", "with")  # Only allow SELECT/WITH statements.
        if not clean_sql.startswith(allowed_prefixes):
            result = ReviewResult(
                stage="Auto review failed",
                errlevel=2,
                stagestatus="Check failed!",
                errormessage="Disallowed statement!",
                affected_rows=actual_rows_check,
                sql=full_sql,
            )
        elif result_set.error:
            result = ReviewResult(
                stage="Auto review failed",
                errlevel=2,
                stagestatus="Check failed!",
                errormessage=result_set.error,
                affected_rows=actual_rows_check,
                sql=full_sql,
            )
        elif actual_rows_check > max_export_rows:
            result = ReviewResult(
                errlevel=2,
                stagestatus="Check failed!",
                errormessage=(
                    f"Export row count ({actual_rows_check}) exceeds threshold "
                    f"({max_export_rows})."
                ),
                affected_rows=actual_rows_check,
                sql=full_sql,
            )
        else:
            result = ReviewResult(
                errlevel=0,
                stagestatus="Row count completed",
                errormessage="None",
                sql=full_sql,
                affected_rows=actual_rows_check,
                execute_time=0,
            )
        check_result.rows = [result]
        check_result.affected_rows = actual_rows_check
        # Count warnings and errors.
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1
        return check_result


def save_to_format_file(
    format_type=None, result=None, workflow=None, columns=None, temp_dir=None
):
    """
    Save query result into a file with specified format.
    :param format_type: File format type (csv/json/xml/xlsx/sql)
    :param result: Query result
    :param workflow: Workflow instance
    :param columns: Column names
    :param temp_dir: Temporary directory path
    :return: Compressed filename
    """
    # Generate unique filename (workflow DB + timestamp + random hash).
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    hash_value = hashlib.sha256(os.urandom(32)).hexdigest()[:8]  # Use first 8 chars.
    base_name = f"{workflow.db_name}_{timestamp}_{hash_value}"
    file_name = f"{base_name}.{format_type}"
    file_path = os.path.join(temp_dir, file_name)
    # Write query result into target format file.
    if format_type == "csv":
        save_csv(file_path, result, columns)
    elif format_type == "tsv":
        save_tsv(file_path, result, columns)
    elif format_type == "json":
        save_json(file_path, result, columns)
    elif format_type == "xml":
        save_xml(file_path, result, columns)
    elif format_type == "xlsx":
        save_xlsx(file_path, result, columns)
    elif format_type == "sql":
        save_sql(file_path, result, columns)
    else:
        raise ValueError(f"Unsupported format type: {format_type}")

    return file_name


def save_delimited(file_path, result, columns, delimiter):
    with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL, delimiter=delimiter)

        if columns:
            csv_writer.writerow(columns)

        for row in result:
            csv_row = ["null" if value is None else value for value in row]
            csv_writer.writerow(csv_row)


def save_csv(file_path, result, columns):
    """
    Save CSV file from query result.
    :param file_path: CSV file path
    :param result: Query result
    :param columns: Column names
    """
    save_delimited(file_path, result, columns, ",")


def save_tsv(file_path, result, columns):
    """
    Save TSV file from query result.
    :param file_path: TSV file path
    :param result: Query result
    :param columns: Column names
    """
    save_delimited(file_path, result, columns, "\t")


def save_json(file_path, result, columns):
    """
    Save JSON file from query result.
    :param file_path: JSON file path
    :param result: Query result
    :param columns: Column names
    """
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(
            [dict(zip(columns, row)) for row in result],
            json_file,
            indent=2,
            ensure_ascii=False,
        )


def save_xml(file_path, result, columns):
    """
    Save XML file from query result.
    :param file_path: XML file path
    :param result: Query result
    :param columns: Column names
    """
    root = ET.Element("tabledata")

    # Create fields element
    fields_elem = ET.SubElement(root, "fields")
    for column in columns:
        field_elem = ET.SubElement(fields_elem, "field")
        field_elem.text = column

    # Create data element
    data_elem = ET.SubElement(root, "data")
    for row_id, row in enumerate(result, start=1):
        row_elem = ET.SubElement(data_elem, "row", id=str(row_id))
        for col_idx, value in enumerate(row, start=1):
            col_elem = ET.SubElement(row_elem, f"column-{col_idx}")
            if value is None:
                col_elem.text = "(null)"
            elif isinstance(value, (datetime.date, datetime.datetime)):
                col_elem.text = value.isoformat()
            else:
                col_elem.text = str(value)

    tree = ET.ElementTree(root)
    tree.write(file_path, encoding="utf-8", xml_declaration=True)


def save_xlsx(file_path, result, columns):
    """
    Save Excel file from query result.
    :param file_path: Excel file path
    :param result: Query result
    :param columns: Column names
    """
    try:
        df = pd.DataFrame(
            [
                [
                    str(value) if value is not None and value != "NULL" else ""
                    for value in row
                ]
                for row in result
            ],
            columns=columns,
        )
        df.to_excel(file_path, index=False, header=True)
    except ValueError as e:
        raise ValueError("Excel supports at most 1048576 rows, limit exceeded!")


def save_sql(file_path, result, columns):
    """
    Save SQL file from query result.
    :param file_path: SQL file path
    :param result: Query result
    :param columns: Column names
    """
    with open(file_path, "w") as sql_file:
        for row in result:
            table_name = "your_table_name"
            if columns:
                sql_file.write(
                    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
                )

            values = ", ".join(
                [
                    (
                        "'{}'".format(str(value).replace("'", "''"))
                        if isinstance(value, str)
                        or isinstance(value, datetime.date)
                        or isinstance(value, datetime.datetime)
                        else "NULL" if value is None or value == "" else str(value)
                    )
                    for value in row
                ]
            )
            sql_file.write(f"({values});\n")


class StorageFileResponse(FileResponse):
    """
    Custom file response class for downloads.
    Mainly used to close backend connections for SFTP storage downloads.
    """

    def __init__(self, *args, storage=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = storage

    def close(self):
        super().close()
        if hasattr(self, "storage") and self.storage:
            self.storage.close()


def download_export_file(request, file_name, workflow_id):
    """
    Download file:
    local/SFTP returns file stream, cloud object storage returns redirect URL.
    :param request:
    :param file_name:
    :param workflow_id:
    :return:
    """
    action = "Offline download"
    extra_info = f"Workflow ID: {workflow_id}, file: {file_name}"
    config = SysConfig()
    storage_type = config.get("storage_type")
    storage = DynamicStorage()

    try:
        if not storage.exists(file_name):
            extra_info = extra_info + ", error: file does not exist."
            return JsonResponse({"error": "File does not exist"}, status=404)
        elif storage.exists(file_name):
            if storage_type in ["sftp", "local"]:
                # SFTP/local handling: return file stream directly.
                try:
                    file = storage.open(file_name, "rb")
                    file_size = storage.size(file_name)
                    response = StorageFileResponse(file, storage=storage)
                    response["Content-Disposition"] = (
                        f'attachment; filename="{file_name}"'
                    )
                    response["Content-Length"] = str(file_size)
                    response["Content-Encoding"] = "identity"
                    return response
                except Exception as e:
                    extra_info = extra_info + f", error: {str(e)}"
                    logger.error(extra_info)
                    return JsonResponse(
                        {"error": "File download failed. Please contact admin."},
                        status=500,
                    )

            elif storage_type in ["s3c", "azure"]:
                try:
                    # Generate presigned URL for cloud object storage.
                    presigned_url = storage.url(file_name)
                    return JsonResponse({"type": "redirect", "url": presigned_url})
                except Exception as e:
                    extra_info = extra_info + f", error: {str(e)}"
                    logger.error(extra_info)
                    return JsonResponse(
                        {"error": "File download failed. Please contact admin."},
                        status=500,
                    )

    except Exception as e:
        extra_info = extra_info + f", error: {str(e)}"
        logger.error(extra_info)
        return JsonResponse(
            {"error": "Internal error, please contact admin."}, status=500
        )

    finally:
        if request.method != "HEAD":
            AuditEntry.objects.create(
                user_id=request.user.id,
                user_name=request.user.username,
                user_display=request.user.display,
                action=action,
                extra_info=extra_info,
            )


def offline_file_download(request):
    """
    Legacy download endpoint wrapper.
    Mirrors WorkflowDownload authorization, but derives the artifact name
    server-side before delegating to download_export_file.
    """
    workflow_id = request.GET.get("workflow_id", "").strip()
    if not workflow_id:
        return JsonResponse({"error": "workflow_id is required"}, status=400)

    workflow = get_object_or_404(SqlWorkflow, pk=workflow_id)

    try:
        if not can_view(request.user, workflow.id):
            raise PermissionDenied("You do not have permission to view this workflow.")
        if not (
            request.user.is_superuser or request.user.has_perm("sql.offline_download")
        ):
            raise PermissionDenied(
                "You do not have permission to download export files."
            )
    except PermissionDenied as exc:
        return JsonResponse({"error": str(exc)}, status=403)

    if not workflow.is_offline_export:
        return JsonResponse(
            {"error": "This workflow does not have an export artifact."}, status=400
        )
    if workflow.status != "workflow_finish" or not workflow.file_name:
        return JsonResponse(
            {"error": "The export artifact is not available yet."}, status=400
        )

    return download_export_file(request, workflow.file_name, workflow.id)
