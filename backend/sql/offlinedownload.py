# -*- coding: UTF-8 -*-
import logging
import os
import csv
import hashlib
import datetime
import xml.etree.ElementTree as ET

import simplejson as json
import pandas as pd
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse, FileResponse
from django.shortcuts import get_object_or_404


from sql.models import SqlWorkflow, AuditEntry
from sql.engines import EngineBase
from sql.storage import DynamicStorage
from sql.utils.sql_review import can_view

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
        raise RuntimeError(
            "Direct export execution is disabled; dispatch export.execute to an agent."
        )

    def pre_count_check(self, workflow):
        """
        Backend checks before workflow submission:
        validate row count threshold and allowed query statements.
        :param workflow: Workflow instance
        :return: Validation result
        """
        raise RuntimeError(
            "Direct export review is disabled; dispatch export.check to an agent."
        )


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
    storage = DynamicStorage()
    storage_type = storage.storage_type

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
    except PermissionDenied:
        return JsonResponse(
            {"error": "You do not have permission to download this file."}, status=403
        )

    if not workflow.is_offline_export:
        return JsonResponse(
            {"error": "This workflow does not have an export artifact."}, status=400
        )
    if workflow.status != "workflow_finish" or not workflow.file_name:
        return JsonResponse(
            {"error": "The export artifact is not available yet."}, status=400
        )

    return download_export_file(request, workflow.file_name, workflow.id)
