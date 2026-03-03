# -*- coding: UTF-8 -*-
import re
import time
import pymongo
import logging
import traceback
import subprocess
import simplejson as json
import datetime
import tempfile
from bson.son import SON
from bson import json_util
from pymongo.errors import OperationFailure
from dateutil.parser import parse
from bson.objectid import ObjectId
from bson.int64 import Int64

from sql.utils.data_masking import data_masking

from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult
from common.config import SysConfig

logger = logging.getLogger("default")

# Local path of mongo client.
mongo = "mongo"


# Custom exception.
class mongo_error(Exception):
    def __init__(self, error_info):
        super().__init__(self)
        self.error_info = error_info

    def __str__(self):
        return self.error_info


class JsonDecoder:
    """Parse MongoDB statement fragments into a pymongo-compatible dict."""

    def __init__(self):
        pass

    def __json_object(self, tokener):
        # obj = collections.OrderedDict()
        obj = {}
        if tokener.cur_token() != "{":
            raise Exception('Json must start with "{"')

        while True:
            tokener.next()
            tk_temp = tokener.cur_token()
            if tk_temp == "}":
                return {}
            # Restrict key format.
            if not isinstance(
                tk_temp, str
            ):  # or (not tk_temp.isidentifier() and not tk_temp.startswith("$"))
                raise Exception("invalid key %s" % tk_temp)
            key = tk_temp.strip()
            tokener.next()
            if tokener.cur_token() != ":":
                raise Exception('expect ":" after "%s"' % key)

            tokener.next()
            val = tokener.cur_token()
            if val == "[":
                val = self.__json_array(tokener)
            elif val == "{":
                val = self.__json_object(tokener)
            obj[key] = val

            tokener.next()
            tk_split = tokener.cur_token()
            if tk_split == ",":
                continue
            elif tk_split == "}":
                break
            else:
                if tk_split is None:
                    raise Exception('missing "}" at at the end of object')
                raise Exception('unexpected token "%s" at key "%s"' % (tk_split, key))
        return obj

    def __json_array(self, tokener):
        if tokener.cur_token() != "[":
            raise Exception('Json array must start with "["')

        arr = []
        while True:
            tokener.next()
            tk_temp = tokener.cur_token()
            if tk_temp == "]":
                return []
            if tk_temp == "{":
                val = self.__json_object(tokener)
            elif tk_temp == "[":
                val = self.__json_array(tokener)
            elif tk_temp in (",", ":", "}"):
                raise Exception('unexpected token "%s"' % tk_temp)
            else:
                val = tk_temp
            arr.append(val)

            tokener.next()
            tk_end = tokener.cur_token()
            if tk_end == ",":
                continue
            if tk_end == "]":
                break
            else:
                if tk_end is None:
                    raise Exception('missing "]" at the end of array')
        return arr

    def decode(self, json_str):
        tokener = JsonDecoder.__Tokener(json_str)
        if not tokener.next():
            return None
        first_token = tokener.cur_token()

        if first_token == "{":
            decode_val = self.__json_object(tokener)
        elif first_token == "[":
            decode_val = self.__json_array(tokener)
        else:
            raise Exception('Json must start with "{"')
        if tokener.next():
            raise Exception('unexpected token "%s"' % tokener.cur_token())
        return decode_val

    class __Tokener:  # Tokener as an inner class.
        def __init__(self, json_str):
            self.__str = json_str
            self.__i = 0
            self.__cur_token = None

        def __cur_char(self):
            if self.__i < len(self.__str):
                return self.__str[self.__i]
            return ""

        def __previous_char(self):
            if self.__i < len(self.__str):
                return self.__str[self.__i - 1]

        def __remain_str(self):
            if self.__i < len(self.__str):
                return self.__str[self.__i :]

        def __move_i(self, step=1):
            if self.__i < len(self.__str):
                self.__i += step

        def __next_string(self):
            """Parse quoted string until matching closing quote is found."""
            outstr = ""
            trans_flag = False
            start_ch = ""
            self.__move_i()
            while self.__cur_char() != "":
                ch = self.__cur_char()
                if start_ch == "":
                    start_ch = self.__previous_char()
                if ch == '\\"':  # Check whether this is an escape character.
                    trans_flag = True
                else:
                    if not trans_flag:
                        if (ch == '"' and start_ch == '"') or (
                            ch == "'" and start_ch == "'"
                        ):
                            break
                    else:
                        trans_flag = False
                outstr += ch
                self.__move_i()
            return outstr

        def __next_number(self):
            expr = ""
            while self.__cur_char().isdigit() or self.__cur_char() in (".", "+", "-"):
                expr += self.__cur_char()
                self.__move_i()
            self.__move_i(-1)
            if "." in expr:
                return float(expr)
            else:
                return int(expr)

        def __next_const(self):
            """Parse unquoted tokens like true and ObjectId."""
            outstr = ""
            data_type = ""
            while self.__cur_char().isalpha() or self.__cur_char() in ("$", "_", " "):
                outstr += self.__cur_char()
                self.__move_i()
                if outstr.replace(" ", "") in (
                    "ObjectId",
                    "newDate",
                    "ISODate",
                    "newISODate",
                    "NumberLong",
                ):  # Similar types may need dedicated handling, e.g. int().
                    data_type = outstr
                    for c in self.__remain_str():
                        outstr += c
                        self.__move_i()
                        if c == ")":
                            break

            self.__move_i(-1)

            if outstr in ("true", "false", "null"):
                return {"true": True, "false": False, "null": None}[outstr]
            elif data_type == "ObjectId":
                ojStr = re.findall(r"ObjectId\(.*?\)", outstr)  # Handle ObjectId.
                if len(ojStr) > 0:
                    # return eval(ojStr[0])
                    id_str = re.findall(r"\(.*?\)", ojStr[0])
                    oid = id_str[0].replace(" ", "")[2:-2]
                    return ObjectId(oid)
            elif data_type.replace(" ", "") in (
                "newDate",
                "ISODate",
                "newISODate",
            ):  # Handle datetime format.
                tmp_type = "%s()" % data_type
                if outstr.replace(" ", "") == tmp_type.replace(" ", ""):
                    return datetime.datetime.now() + datetime.timedelta(
                        hours=-8
                    )  # MongoDB default timezone is UTC.
                date_regex = re.compile(r'%s\("(.*)"\)' % data_type, re.IGNORECASE)
                date_content = date_regex.findall(outstr)
                if len(date_content) > 0:
                    return parse(date_content[0], yearfirst=True)
            elif data_type.replace(" ", "") in ("NumberLong",):
                nuStr = re.findall(r"NumberLong\(.*?\)", outstr)  # Handle NumberLong.
                if len(nuStr) > 0:
                    id_str = re.findall(r"\(.*?\)", nuStr[0])
                    nlong = id_str[0].replace(" ", "")[2:-2]
                    return Int64(nlong)
            elif outstr:
                return outstr
            raise Exception('Invalid symbol "%s"' % outstr)

        def next(self):
            is_white_space = lambda a_char: a_char in (
                "\x20",
                "\n",
                "\r",
                "\t",
            )  # Define a lambda function.

            while is_white_space(self.__cur_char()):
                self.__move_i()

            ch = self.__cur_char()
            if ch == "":
                cur_token = None
            elif ch in ("{", "}", "[", "]", ",", ":"):
                cur_token = ch
            elif ch in ('"', "'"):  # Character is quote.
                cur_token = self.__next_string()
            elif ch.isalpha() or ch in ("$", "_"):  # Alpha / "$" / "_" token.
                cur_token = self.__next_const()
            elif ch.isdigit() or ch in (".", "-", "+"):  # Numeric token.
                cur_token = self.__next_number()
            else:
                raise Exception('Invalid symbol "%s"' % ch)
            self.__move_i()
            self.__cur_token = cur_token

            return cur_token is not None

        def cur_token(self):
            return self.__cur_token


class MongoEngine(EngineBase):
    error = None
    warning = None
    methodStr = None

    def test_connection(self):
        return self.get_all_databases()

    def exec_cmd(self, sql, db_name=None, slave_ok=""):
        """Execute statement used during review."""

        if self.port and self.host:
            msg = ""
            auth_db = self.instance.db_name or "admin"
            sql_len = len(sql)
            is_load = False  # Default: do not execute via load().
            try:
                if not sql.startswith("var host=") and sql_len > 4000:
                    # On master node, use load-js approach when SQL is too long.
                    # Rebuild SQL so js execution result can be echoed.
                    sql = "var result = " + sql + "\nprintjson(result);"
                    # Use NamedTemporaryFile so file path is known.
                    fp = tempfile.NamedTemporaryFile(
                        suffix=".js", prefix="mongo_", dir="/tmp/", delete=True
                    )
                    fp.write(sql.encode("utf-8"))
                    fp.seek(0)  # Rewind pointer so content is flushed to disk.
                    cmd = self._build_cmd(
                        db_name, auth_db, slave_ok, fp.name, is_load=True
                    )
                    is_load = True  # Mark load mode, used in finally cleanup.
                elif (
                    not sql.startswith("var host=") and sql_len < 4000
                ):  # On master node, execute directly via mongo shell.
                    cmd = self._build_cmd(db_name, auth_db, slave_ok, sql=sql)
                else:
                    cmd = self._build_cmd(
                        db_name, auth_db, sql=sql, slave_ok="rs.slaveOk();"
                    )
                p = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                re_msg = []
                for line in iter(p.stdout.read, ""):
                    re_msg.append(line)
                # Returned lines may contain newlines, convert all to one string first.
                __msg = "\n".join(re_msg)
                _re_msg = []
                for _line in __msg.split("\n"):
                    if not _re_msg and re.match("WARNING.*", _line):
                        # The first line may be a WARNING line, skip it.
                        continue
                    _re_msg.append(_line)

                msg = "\n".join(_re_msg)
                msg = msg.replace("true\n", "")
            except Exception as e:
                logger.warning(
                    f"Mongo statement execution failed, SQL: {sql}, "
                    f"error: {e}, traceback: {traceback.format_exc()}"
                )
            finally:
                if is_load:
                    fp.close()
        return msg

    # Build mongo command based on auth and whether a temp file is required.
    def _build_cmd(
        self, db_name, auth_db, slave_ok="", tempfile_=None, sql=None, is_load=False
    ):
        # Extract common parameters.
        common_params = {
            "mongo": "mongo",
            "host": self.host,
            "port": self.port,
            "db_name": db_name,
            "auth_db": auth_db,
            "slave_ok": slave_ok,
        }
        if is_load:
            cmd_template = (
                "{mongo} --quiet {auth_options} {host}:{port}/{auth_db} <<\\EOF\n"
                "db=db.getSiblingDB('{db_name}');{slave_ok}load('{tempfile_}')\nEOF"
            )
            # Use load-js mode with temp file for over-limit query length.
            common_params["tempfile_"] = tempfile_
        else:
            cmd_template = (
                "{mongo} --quiet {auth_options} {host}:{port}/{auth_db} <<\\EOF\n"
                "db=db.getSiblingDB('{db_name}');{slave_ok}{sql}\nEOF"
            )
            # Use direct mongo shell execution when under the length limit.
            common_params["sql"] = sql
        # Add auth options when username/password are provided.
        if self.user and self.password:
            common_params["auth_options"] = "-u {uname} -p '{password}'".format(
                uname=self.user, password=self.password
            )
        else:
            common_params["auth_options"] = ""
        return cmd_template.format(**common_params)

    def get_master(self):
        """Get host and port of primary node."""

        sql = "rs.isMaster().primary"
        master = self.exec_cmd(sql)
        if master != "undefined":
            sp_host = master.replace('"', "").split(":")
            self.host = sp_host[0]
            self.port = int(sp_host[1])
        # return master

    def get_slave(self):
        """Get host and port of secondary node."""

        sql = """var host=""; rs.status().members.forEach(function(item) {i=1; if (item.stateStr =="SECONDARY") \
        {host=item.name } }); print(host);"""
        slave_msg = self.exec_cmd(sql, db_name=self.db_name)
        # On some cloud MongoDB (e.g. Aliyun), real secondary host:port may be
        # unavailable. For such cases, fall back to primary execution.
        # If value has no colon, treat it as cloud label and return False.
        if ":" not in slave_msg:
            return False
        if slave_msg.lower().find("undefined") < 0:
            sp_host = slave_msg.replace('"', "").split(":")
            self.host = sp_host[0]
            self.port = int(sp_host[1])
            return True
        else:
            return False

    def get_table_conut(self, table_name, db_name):
        try:
            count_sql = f"db.{table_name}.count()"
            status = self.get_slave()  # Count query should run on secondary.
            if self.host and self.port and status:
                count = int(self.exec_cmd(count_sql, db_name, slave_ok="rs.slaveOk();"))
            else:
                count = int(self.exec_cmd(count_sql, db_name))
            return count
        except Exception as e:
            logger.debug("get_table_conut:" + str(e))
            return 0

    def execute_workflow(self, workflow):
        """Execute workflow, return ReviewSet."""
        return self.execute(
            db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content
        )

    def execute(self, db_name=None, sql=""):
        """Execute mongo command statement."""
        self.get_master()
        execute_result = ReviewSet(full_sql=sql)
        sql = sql.strip()
        # Split by semicolon and execute one by one.
        sp_sql = sql.split(";")
        line = 0
        for exec_sql in sp_sql:
            if not exec_sql == "":
                exec_sql = exec_sql.strip()
                try:
                    # DeprecationWarning: time.clock has been deprecated in Python 3.3 and will be removed from Python 3.8: use time.perf_counter or time.process_time instead
                    start = time.perf_counter()
                    r = self.exec_cmd(exec_sql, db_name)
                    end = time.perf_counter()
                    line += 1
                    logger.debug("Execution result: " + r)
                    # Handle execution errors.
                    rz = r.replace(" ", "").replace('"', "")
                    tr = 1
                    if (
                        r.lower().find("syntaxerror") >= 0
                        or rz.find("ok:0") >= 0
                        or rz.find("error:invalid") >= 0
                        or rz.find("ReferenceError") >= 0
                        or rz.find("getErrorWithCode") >= 0
                        or rz.find("failedtoconnect") >= 0
                        or rz.find("Error:") >= 0
                    ):
                        tr = 0
                    if (rz.find("errmsg") >= 0 or tr == 0) and (
                        r.lower().find("already exist") < 0
                    ):
                        execute_result.error = r
                        result = ReviewResult(
                            id=line,
                            stage="Execute failed",
                            errlevel=2,
                            stagestatus="Aborted unexpectedly",
                            errormessage=f"Mongo statement execution failed: {r}",
                            sql=exec_sql,
                        )
                    else:
                        try:
                            r = json.loads(r)
                        except Exception as e:
                            logger.info(str(e))
                        finally:
                            methodStr = exec_sql.split(").")[-1].split("(")[0].strip()
                            if "." in methodStr:
                                methodStr = methodStr.split(".")[-1]
                            if methodStr == "insert":
                                m = re.search(r'"nInserted"\s*:\s*(\d+)', r)
                                actual_affected_rows = int(m.group(1))
                            elif methodStr in ("insertOne", "insertMany"):
                                if isinstance(r, dict):
                                    # mongosh / driver JSON formats
                                    if "nInserted" in r:  # BulkWriteResult style
                                        actual_affected_rows = r["nInserted"]
                                    elif (
                                        "insertedIds" in r
                                    ):  # CLI acknowledged + insertedIds
                                        actual_affected_rows = len(r["insertedIds"])
                                    elif "insertedId" in r:  # insertOne single id
                                        actual_affected_rows = 1
                                    else:
                                        actual_affected_rows = 0
                                elif isinstance(r, str):
                                    # mongo 4.x CLI string outputs
                                    m = re.search(r'"nInserted"\s*:\s*(\d+)', r)
                                    actual_affected_rows = (
                                        int(m.group(1)) if m else r.count("ObjectId")
                                    )
                                    actual_affected_rows = r.count("ObjectId")
                                else:
                                    actual_affected_rows = 0
                            elif methodStr == "update":
                                m = re.search(
                                    r'(?:"modifiedCount"|"nModified")\s*:\s*(\d+)',
                                    r,
                                )
                                actual_affected_rows = int(m.group(1))
                            elif methodStr in ("updateOne", "updateMany"):
                                if isinstance(r, dict):
                                    actual_affected_rows = r.get(
                                        "modifiedCount", r.get("nModified", 0)
                                    )
                                elif isinstance(r, str):
                                    m = re.search(
                                        r'(?:"modifiedCount"|"nModified")\s*:\s*(\d+)',
                                        r,
                                    )
                                    actual_affected_rows = int(m.group(1)) if m else 0
                                else:
                                    actual_affected_rows = 0
                            elif methodStr in ("deleteOne", "deleteMany"):
                                actual_affected_rows = r.get("deletedCount", 0)
                            elif methodStr == "remove":
                                actual_affected_rows = r.get("nRemoved", 0)
                            else:
                                actual_affected_rows = 0
                        # Convert result to ReviewResult.
                        result = ReviewResult(
                            id=line,
                            errlevel=0,
                            stagestatus="Execution finished",
                            errormessage=str(r),
                            execute_time=round(end - start, 6),
                            affected_rows=actual_affected_rows,
                            sql=exec_sql,
                        )
                    execute_result.rows += [result]
                except Exception as e:
                    logger.warning(
                        f"Mongo statement execution failed, SQL: {exec_sql}, "
                        f"traceback: {traceback.format_exc()}"
                    )
                    execute_result.error = str(e)
            # result_set.column_list = [i[0] for i in fields] if fields else []
        return execute_result

    def execute_check(self, db_name=None, sql=""):
        """Pre-check before workflow execution, return ReviewSet."""
        line = 1
        count = 0
        check_result = ReviewSet(full_sql=sql)

        # Get real_row_count config option.
        real_row_count = SysConfig().get("real_row_count", False)

        sql = sql.strip()
        # Remove SQL comment lines for check.
        sql = re.sub(r"^\s*//.*$", "", sql, flags=re.MULTILINE)
        if sql.find(";") < 0:
            raise Exception("Submitted statement must end with semicolon")
        # Split by semicolon and process one by one.
        sp_sql = sql.split(";")
        # Process statements.
        for check_sql in sp_sql:
            alert = ""  # Warning message.
            check_sql = check_sql.strip()
            if not check_sql == "" and check_sql != "\n":
                # check_sql = f'''{check_sql}'''
                # check_sql = check_sql.replace('\n', '')  # flatten to one line
                # Supported command list.
                supportMethodList = [
                    "explain",
                    "bulkWrite",
                    "convertToCapped",
                    "createIndex",
                    "createIndexes",
                    "deleteOne",
                    "deleteMany",
                    "drop",
                    "dropIndex",
                    "dropIndexes",
                    "ensureIndex",
                    "insert",
                    "insertOne",
                    "insertMany",
                    "remove",
                    "replaceOne",
                    "renameCollection",
                    "update",
                    "updateOne",
                    "updateMany",
                    "createCollection",
                    "renameCollection",
                ]
                # Methods that require existing collection.
                is_exist_premise_method = [
                    "convertToCapped",
                    "deleteOne",
                    "deleteMany",
                    "drop",
                    "dropIndex",
                    "dropIndexes",
                    "remove",
                    "replaceOne",
                    "renameCollection",
                    "update",
                    "updateOne",
                    "updateMany",
                    "renameCollection",
                ]
                pattern = re.compile(
                    r"""^db\.createCollection\(([\s\S]*)\)$|^db\.([\w\.-]+)\.(?:[A-Za-z]+)(?:\([\s\S]*\)$)|^db\.getCollection\((?:\s*)(?:'|")([\w\.-]+)('|")(\s*)\)\.([A-Za-z]+)(\([\s\S]*\)$)"""
                )
                m = pattern.match(check_sql)
                if (
                    m is not None
                    and (re.search(re.compile(r"}(?:\s*){"), check_sql) is None)
                    and check_sql.count("{") == check_sql.count("}")
                    and check_sql.count("(") == check_sql.count(")")
                ):
                    sql_str = m.group()
                    table_name = (
                        m.group(1) or m.group(2) or m.group(3)
                    ).strip()  # Get collection name from regex groups.
                    table_name = table_name.replace('"', "").replace("'", "")
                    table_names = self.get_all_tables(db_name).rows
                    is_in = table_name in table_names  # Check collection existence.
                    if not is_in:
                        alert = f"\nTip: collection `{table_name}` does not exist!"
                    if sql_str:
                        count = 0
                        if (
                            sql_str.find("createCollection") > 0
                        ):  # If method is db.createCollection().
                            methodStr = "createCollection"
                            alert = ""
                            if is_in:
                                check_result.error = "Collection already exists"
                                result = ReviewResult(
                                    id=line,
                                    errlevel=2,
                                    stagestatus="Collection already exists",
                                    errormessage="Collection already exists!",
                                    affected_rows=count,
                                    sql=check_sql,
                                )
                                check_result.rows += [result]
                                continue
                        else:
                            methodStr = sql_str.split(").")[-1].split("(")[0].strip()
                            if "." in methodStr:
                                methodStr = methodStr.split(".")[-1]
                        if methodStr in is_exist_premise_method and not is_in:
                            check_result.error = "Collection does not exist"
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus="Collection does not exist",
                                errormessage=(
                                    f"Collection does not exist, cannot perform "
                                    f"`{methodStr}` operation!"
                                ),
                                sql=check_sql,
                            )
                            check_result.rows += [result]
                            continue
                        if methodStr in supportMethodList:  # Check method support.
                            if (
                                methodStr == "createIndex"
                                or methodStr == "createIndexes"
                                or methodStr == "ensureIndex"
                            ):  # For index creation on >5M docs, show warning.
                                p_back = re.compile(
                                    r"""(['"])(?:(?!\1)background)\1(?:\s*):(?:\s*)true|background\s*:\s*true|(['"])(?:(?!\1)background)\1(?:\s*):(?:\s*)(['"])(?:(?!\2)true)\2""",
                                    re.M,
                                )
                                m_back = re.search(p_back, check_sql)
                                if m_back is None:
                                    count = 5555555
                                    check_result.warning = (
                                        "Please add `background:true` "
                                        "when creating index"
                                    )
                                    check_result.warning_count += 1
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=2,
                                        stagestatus="Background index creation",
                                        errormessage=(
                                            "Index creation does not include "
                                            "`background:true`"
                                        )
                                        + alert,
                                        sql=check_sql,
                                    )
                                elif not is_in:
                                    count = 0
                                else:
                                    count = self.get_table_conut(
                                        table_name, db_name
                                    )  # Get total document count.
                                    if count >= 5000000:
                                        check_result.warning = (
                                            alert + "More than 5 million documents, "
                                            "please create index during low traffic"
                                        )
                                        check_result.warning_count += 1
                                        result = ReviewResult(
                                            id=line,
                                            errlevel=1,
                                            stagestatus="Large collection index creation",
                                            errormessage=(
                                                "More than 5 million documents, "
                                                "please create index during low traffic!"
                                            ),
                                            affected_rows=count,
                                            sql=check_sql,
                                        )
                            if count < 5000000:
                                # Check passed.
                                affected_all_row_method = [
                                    "drop",
                                    "dropIndex",
                                    "dropIndexes",
                                    "createIndex",
                                    "createIndexes",
                                    "ensureIndex",
                                ]
                                if methodStr not in affected_all_row_method:
                                    count = 0
                                else:
                                    count = self.get_table_conut(
                                        table_name, db_name
                                    )  # Get total document count.
                                result = ReviewResult(
                                    id=line,
                                    errlevel=0,
                                    stagestatus="Audit completed",
                                    errormessage="Check passed",
                                    affected_rows=count,
                                    sql=check_sql,
                                    execute_time=0,
                                )
                            if real_row_count:
                                if methodStr == "insertOne":
                                    count = 1
                                elif methodStr in ("insert", "insertMany"):
                                    insert_str = re.search(
                                        rf"{methodStr}\((.*)\)", sql_str, re.S
                                    ).group(1)
                                    first_char = insert_str.replace(" ", "").replace(
                                        "\n", ""
                                    )[0]
                                    if first_char == "{":
                                        count = 1
                                    elif first_char == "[":
                                        insert_values = re.search(
                                            r"\[(.*?)\]", insert_str, re.S
                                        ).group(0)
                                        de = JsonDecoder()
                                        insert_values = de.decode(insert_values)
                                        count = len(insert_values)
                                    else:
                                        count = 0
                                elif methodStr in (
                                    "update",
                                    "updateOne",
                                    "updateMany",
                                    "deleteOne",
                                    "deleteMany",
                                    "remove",
                                ):
                                    if sql_str.find("find(") > 0:
                                        count_sql = sql_str.replace(methodStr, "count")
                                    else:
                                        count_sql = (
                                            sql_str.replace(methodStr, "find")
                                            + ".count()"
                                        )
                                    query_dict = self.parse_query_sentence(count_sql)
                                    count_sql = f"""db.getCollection("{query_dict["collection"]}").find({query_dict["condition"]}).count()"""
                                    query_result = self.query(db_name, count_sql)
                                    count = json.loads(query_result.rows[0][0]).get(
                                        "count", 0
                                    )
                                    if (
                                        methodStr == "update"
                                        and "multi:true"
                                        not in sql_str.replace(" ", "")
                                        .replace('"', "")
                                        .replace("'", "")
                                        .replace("\n", "")
                                    ) or methodStr in ("deleteOne", "updateOne"):
                                        count = 1 if count > 0 else 0
                            if methodStr in (
                                "insertOne",
                                "insert",
                                "insertMany",
                                "update",
                                "updateOne",
                                "updateMany",
                                "deleteOne",
                                "deleteMany",
                                "remove",
                            ):
                                result = ReviewResult(
                                    id=line,
                                    errlevel=0,
                                    stagestatus="Audit completed",
                                    errormessage="Check passed",
                                    affected_rows=count,
                                    sql=check_sql,
                                    execute_time=0,
                                )
                        else:
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus="Rejected unsupported statement",
                                errormessage=(
                                    "Only DML and DDL statements are supported. "
                                    "Use database query feature for queries!"
                                ),
                                sql=check_sql,
                            )
                else:
                    check_result.error = "Syntax error"
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Syntax error",
                        errormessage=(
                            "Please check statement syntax or whether () {} and },{ "
                            "are correctly matched!"
                        ),
                        sql=check_sql,
                    )
                check_result.rows += [result]
                line += 1
                count = 0
        check_result.column_list = ["Result"]  # Result column name for review.
        check_result.checked = True
        check_result.warning = self.warning
        # Count warnings and errors.
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1
        return check_result

    def get_connection(self, db_name=None):
        self.db_name = db_name or self.instance.db_name or "admin"
        auth_db = self.instance.db_name or "admin"

        options = {
            "host": self.host,
            "port": self.port,
            "username": self.user,
            "password": self.password,
            "authSource": auth_db,
            "connect": True,
            "connectTimeoutMS": 10000,
        }

        # only set TLS options while the instance enabled the TLS, to avoid
        # tlsInsecure option being set but the instance is not enabled the TLS
        # which would cause pymongo.ConfigurationError
        if self.instance.is_ssl:
            options["tls"] = True
            options["tlsInsecure"] = not self.instance.verify_ssl

        if self.user and self.password:
            self.conn = pymongo.MongoClient(**options)
        else:
            self.conn = pymongo.MongoClient(**options)

        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    name = "Mongo"

    info = "Mongo engine"

    def get_roles(self):
        sql_get_roles = "db.system.roles.find({},{_id:1})"
        result_set = self.query("admin", sql_get_roles)
        rows = ["read", "readWrite", "userAdminAnyDatabase"]
        for row in result_set.rows:
            rows.append(row[1])
        result_set.rows = rows
        return result_set

    def get_all_databases(self):
        result = ResultSet()
        conn = self.get_connection()
        try:
            db_list = conn.list_database_names()
        except OperationFailure:
            db_list = [self.db_name]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        result = ResultSet()
        conn = self.get_connection()
        db = conn[db_name]
        result.rows = db.list_collection_names()
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all fields, return a ResultSet."""
        # https://github.com/getredash/redash/blob/master/redash/query_runner/mongodb.py
        result = ResultSet()
        db = self.get_connection()[db_name]
        collection_name = tb_name
        documents_sample = []
        if "viewOn" in db[collection_name].options():
            for d in db[collection_name].find().limit(2):
                documents_sample.append(d)
        else:
            for d in db[collection_name].find().sort([("_id", 1)]).limit(1):
                documents_sample.append(d)

            for d in db[collection_name].find().sort([("_id", -1)]).limit(1):
                documents_sample.append(d)
        columns = []
        # _merge_property_names
        for document in documents_sample:
            for prop in document:
                if prop not in columns:
                    columns.append(prop)
        result.column_list = ["COLUMN_NAME"]
        result.rows = columns
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """Return ResultSet for table description."""
        result = self.get_all_columns_by_tb(db_name=db_name, tb_name=tb_name)
        result.rows = [
            [
                [r],
            ]
            for r in result.rows
        ]
        return result

    @staticmethod
    def dispose_str(parse_sql, start_flag, index):
        """Parse and process string token."""

        stop_flag = ""
        while index < len(parse_sql):
            if parse_sql[index] == stop_flag and parse_sql[index - 1] != "\\":
                return index
            index += 1
            stop_flag = start_flag
        raise Exception("near column %s,' or \" has no close" % index)

    def dispose_pair(self, parse_sql, index, begin, end):
        """Parse paired characters {}[]().
        Increment counter for left bracket and decrement for right bracket.
        """

        start_pos = -1
        stop_pos = 0
        count = 0
        while index < len(parse_sql):
            char = parse_sql[index]
            if char == begin:
                count += 1
                if start_pos == -1:
                    start_pos = index
            if char == end:
                count -= 1
                if count == 0:
                    stop_pos = index + 1
                    break
            if char in ("'", '"'):  # Avoid brackets inside string values.
                index = self.dispose_str(parse_sql, char, index)
            index += 1
        if count > 0:
            raise Exception(
                "near column %s, The symbol %s has no closed" % (index, begin)
            )

        re_char = parse_sql[start_pos:stop_pos]  # Slice matched content.
        return index, re_char

    def parse_query_sentence(self, parse_sql):
        """Parse MongoDB query statement and return a dict."""

        index = 0
        query_dict = {}

        # Start parsing query statement.
        while index < len(parse_sql):
            char = parse_sql[index]
            if char == "(":
                # Get method name in statement.
                head_sql = parse_sql[:index]
                method = parse_sql[:index].split(".")[-1].strip()
                index, re_char = self.dispose_pair(parse_sql, index, "(", ")")
                re_char = re_char.lstrip("(").rstrip(")")
                # Get collection name.
                if method and "collection" not in query_dict:
                    collection = head_sql.replace("." + method, "").replace("db.", "")
                    query_dict["collection"] = collection
                # Split query condition and projection (returned fields).
                if method == "find":
                    p_index, condition = self.dispose_pair(re_char, 0, "{", "}")
                    query_dict["condition"] = condition
                    query_dict["method"] = method
                    # Get projection fields.
                    projection = re_char[p_index:].strip()[2:]
                    if projection:
                        query_dict["projection"] = projection
                # Aggregate query.
                elif method == "aggregate":
                    pipeline = []
                    agg_index = 0
                    while agg_index < len(re_char):
                        p_index, condition = self.dispose_pair(
                            re_char, agg_index, "{", "}"
                        )
                        agg_index = p_index + 1
                        if condition:
                            de = JsonDecoder()
                            step = de.decode(condition)
                            if "$sort" in step:
                                sort_list = []
                                for name, direction in step["$sort"].items():
                                    sort_list.append((name, direction))
                                step["$sort"] = SON(sort_list)
                            pipeline.append(step)
                        query_dict["condition"] = pipeline
                        query_dict["method"] = method
                elif method.lower() == "getcollection":  # Get collection name.
                    collection = re_char.strip().replace("'", "").replace('"', "")
                    query_dict["collection"] = collection
                elif method.lower() == "getindexes":
                    query_dict["method"] = "index_information"
                else:
                    query_dict[method] = re_char
            index += 1

        logger.debug(query_dict)
        if query_dict:
            return query_dict

    def filter_sql(self, sql="", limit_num=0):
        """Rewrite query statement and return rewritten SQL."""
        sql = sql.split(";")[0].strip()
        # Execution plan.
        if sql.startswith("explain"):
            sql = sql.replace("explain", "") + ".explain()"
        return sql.strip()

    def query_check(self, db_name=None, sql=""):
        """Check query before submission."""

        sql = sql.strip()
        sql = re.sub(r"^\s*//.*$", "", sql, flags=re.MULTILINE)
        if sql.startswith("explain"):
            sql = sql[7:] + ".explain()"
            sql = re.sub(r"[;\s]*\.explain\(\)$", ".explain()", sql).strip()
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        pattern = re.compile(
            r"""^db\.(\w+\.?)+(?:\([\s\S]*\)(\s*;*)$)|^db\.getCollection\((?:\s*)(?:'|")(\w+\.?)+('|")(\s*)\)\.([A-Za-z]+)(\([\s\S]*\)(\s*;*)$)"""
        )
        m = pattern.match(sql)
        if m is not None:
            logger.debug(sql)
            query_dict = self.parse_query_sentence(sql)
            if "method" not in query_dict:
                result["msg"] += "Error: only query-related methods are supported"
                result["bad_query"] = True
                return result
            collection_name = query_dict["collection"]
            collection_names = self.get_all_tables(db_name).rows
            is_in = collection_name in collection_names  # Check collection exists.
            if not is_in:
                result[
                    "msg"
                ] += f"\nError: collection `{collection_name}` does not exist!"
                result["bad_query"] = True
                return result
        else:
            result["msg"] += "Please check statement syntax and use native query syntax"
            result["bad_query"] = True
        return result

    def query(self, db_name=None, sql="", limit_num=0, close_conn=True, **kwargs):
        """Execute query."""

        result_set = ResultSet(full_sql=sql)
        find_cmd = ""

        # Parse content inside () for command segments.
        query_dict = self.parse_query_sentence(sql)
        # Create parser instance.
        de = JsonDecoder()

        collection_name = query_dict["collection"]
        if "method" in query_dict and query_dict["method"]:
            method = query_dict["method"]
            find_cmd = "collection." + method
            if method == "index_information":
                find_cmd += "()"
        if "condition" in query_dict:
            if method == "aggregate":
                condition = query_dict["condition"]
                # Add limit to aggregate query to avoid oversized result set.
                condition.append({"$limit": limit_num})
            if method == "find":
                condition = de.decode(query_dict["condition"])
            find_cmd += "(condition)"
        if "projection" in query_dict and query_dict["projection"]:
            projection = de.decode(query_dict["projection"])
            find_cmd = find_cmd[:-1] + ",projection)"
        if "sort" in query_dict and query_dict["sort"]:
            sorting = []
            for k, v in de.decode(query_dict["sort"]).items():
                sorting.append((k, v))
            find_cmd += ".sort(sorting)"
        if (
            method == "find"
            and "limit" not in query_dict
            and "explain" not in query_dict
        ):
            find_cmd += ".limit(limit_num)"
        if "limit" in query_dict and query_dict["limit"]:
            query_limit = int(query_dict["limit"])
            limit = min(limit_num, query_limit) if query_limit else limit_num
            find_cmd += f".limit({limit})"
        if "skip" in query_dict and query_dict["skip"]:
            query_skip = int(query_dict["skip"])
            find_cmd += f".skip({query_skip})"
        if "count" in query_dict:
            if condition:
                find_cmd = "collection.count_documents(condition)"
            else:
                find_cmd = "collection.count_documents({})"
        if "explain" in query_dict:
            find_cmd += ".explain()"

        try:
            conn = self.get_connection()
            db = conn[db_name]
            collection = db[collection_name]

            # Execute query command.
            logger.debug(find_cmd)
            cursor = eval(find_cmd)

            columns = []
            rows = []
            if "count" in query_dict:
                columns.append("count")
                rows.append({"count": cursor})
            elif "explain" in query_dict:  # Build execution plan result.
                columns.append("explain")
                cursor = json.loads(json_util.dumps(cursor))  # Convert bson to json.
                for k, v in cursor.items():
                    if k not in ("serverInfo", "ok"):
                        rows.append({k: v})
            elif method == "index_information":  # Build index result set.
                columns.append("index_list")
                for k, v in cursor.items():
                    rows.append({k: v})
            elif (
                method == "aggregate" and sql.find("$group") >= 0
            ):  # Build aggregate data.
                row = []
                columns.insert(0, "mongodballdata")
                for ro in cursor:
                    json_col = json.dumps(
                        ro, ensure_ascii=False, indent=2, separators=(",", ":")
                    )
                    row.insert(0, json_col)
                    for k, v in ro.items():
                        if k not in columns:
                            columns.append(k)
                        row.append(v)
                    rows.append(tuple(row))
                    row.clear()
                rows = tuple(rows)
                result_set.rows = rows
            else:
                cursor = json.loads(json_util.dumps(cursor))
                cols = projection if "projection" in dir() else None
                rows, columns = self.parse_tuple(cursor, db_name, collection_name, cols)
                result_set.rows = rows
            result_set.column_list = columns
            result_set.affected_rows = len(rows)
            if isinstance(rows, list):
                logger.debug(rows)
                result_set.rows = tuple(
                    [json.dumps(x, ensure_ascii=False, indent=2, separators=(",", ":"))]
                    for x in rows
                )

        except Exception as e:
            logger.warning(
                f"Mongo command execution failed, SQL: {sql}, "
                f"error: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def parse_tuple(self, cursor, db_name, tb_name, projection=None):
        """Convert mongo query results to tuple format for bootstrap-table."""
        columns = []
        rows = []
        row = []
        if projection:
            for k in projection.keys():
                columns.append(k)
        else:
            result = self.get_all_columns_by_tb(db_name=db_name, tb_name=tb_name)
            columns = result.rows
        columns.insert(0, "mongodballdata")  # Hidden JSON result column.
        columns = self.fill_query_columns(cursor, columns)

        for ro in cursor:
            json_col = json.dumps(
                ro, ensure_ascii=False, indent=2, separators=(",", ":")
            )
            row.insert(0, json_col)
            for key in columns[1:]:
                if key in ro:
                    value = ro[key]
                    if isinstance(value, list):
                        value = "(array) %d Elements" % len(value)
                    re_oid = re.compile(r"{\'\$oid\': \'[0-9a-f]{24}\'}")
                    re_date = re.compile(r"{\'\$date\': [0-9]{13}}")
                    # Convert $oid.
                    ff = re.findall(re_oid, str(value))
                    for ii in ff:
                        value = str(value).replace(
                            ii, "ObjectId(" + ii.split(":")[1].strip()[:-1] + ")"
                        )
                    # Convert $date timestamp.
                    dd = re.findall(re_date, str(value))
                    for d in dd:
                        t = int(d.split(":")[1].strip()[:-1])
                        e = datetime.datetime.fromtimestamp(t / 1000)
                        value = str(value).replace(
                            d, e.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        )
                    row.append(str(value))
                else:
                    row.append("(N/A)")
            rows.append(tuple(row))
            row.clear()
        return tuple(rows), columns

    @staticmethod
    def fill_query_columns(cursor, columns):
        """Add missing fields not returned by `get_all_columns_by_tb`."""
        cols = columns
        for ro in cursor:
            for key in ro.keys():
                if key not in cols:
                    cols.append(key)
        return cols

    def processlist(self, command_type, **kwargs):
        """
        Get current connection information.

        command_type:
        Full    Includes active/inactive and internal connections.
        All     Includes active/inactive, excludes internal connections.
        Active  Includes active connections only.
        Inner   Internal connections only.
        """
        result_set = ResultSet(
            full_sql='db.aggregate([{"$currentOp": {"allUsers":true, "idleConnections":true}}])'
        )
        try:
            conn = self.get_connection()
            processlists = []
            if not command_type:
                command_type = "Active"
            if command_type in ["Full", "All", "Inner"]:
                idle_connections = True
            else:
                idle_connections = False

            # `conn.admin.current_op()` was deprecated in pymongo.
            # MongoDB 3.6+ supports aggregate for current operations.
            with conn.admin.aggregate(
                [
                    {
                        "$currentOp": {
                            "allUsers": True,
                            "idleConnections": idle_connections,
                        }
                    }
                ]
            ) as cursor:
                for operation in cursor:
                    # Special handling for sharding cluster.
                    if "client" not in operation and operation.get(
                        "clientMetadata", {}
                    ).get("mongos", {}).get("client", {}):
                        operation["client"] = operation["clientMetadata"]["mongos"][
                            "client"
                        ]

                    # Get username for this session.
                    effective_users_key = "effectiveUsers_user"
                    effective_users = operation.get("effectiveUsers", [])
                    if isinstance(effective_users, list) and effective_users:
                        first_user = effective_users[0]
                        if isinstance(first_user, dict):
                            operation[effective_users_key] = first_user.get("user", [])
                        else:
                            operation[effective_users_key] = None
                    else:
                        operation[effective_users_key] = None

                    # client_s is from mongos handling, not always the real client.
                    # client may be unavailable in sharding.
                    if command_type in ["Full"]:
                        processlists.append(operation)
                    elif command_type in ["All", "Active"]:
                        if "clientMetadata" in operation:
                            processlists.append(operation)
                    elif command_type in ["Inner"]:
                        if not "clientMetadata" in operation:
                            processlists.append(operation)

            result_set.rows = processlists
        except Exception as e:
            logger.warning(
                f"MongoDB processlist fetch failed, error: {traceback.format_exc()}"
            )
            result_set.error = str(e)

        return result_set

    def get_kill_command(self, opids):
        """Generate kill string from input opid list."""
        conn = self.get_connection()
        active_opid = []
        with conn.admin.aggregate(
            [{"$currentOp": {"allUsers": True, "idleConnections": False}}]
        ) as cursor:
            for operation in cursor:
                if "opid" in operation and operation["opid"] in opids:
                    active_opid.append(operation["opid"])

        kill_command = ""
        for opid in active_opid:
            if isinstance(opid, int):
                kill_command = kill_command + "db.killOp({});".format(opid)
            else:
                kill_command = kill_command + 'db.killOp("{}");'.format(opid)

        return kill_command

    def kill_op(self, opids):
        """kill"""
        result = ResultSet()
        try:
            conn = self.get_connection()
        except Exception as e:
            logger.error(f"{self.name} connection failed, error: {str(e)}")
            result.error = str(e)
            return result
        for opid in opids:
            try:
                conn.admin.command({"killOp": 1, "op": opid})
            except Exception as e:
                sql = {"killOp": 1, "op": opid}
                logger.warning(
                    f"{self.name} killOp failed, command: db.runCommand({sql}), "
                    f"error: {traceback.format_exc()}"
                )
                result.error = str(e)
        return result

    def get_all_databases_summary(self):
        """Instance DB management: get summary for all databases."""
        query_result = self.get_all_databases()
        if not query_result.error:
            dbs = query_result.rows
            conn = self.get_connection()

            # Get database user info.
            rows = []
            for db_name in dbs:
                # Execute command.
                listing = conn[db_name].command(command="usersInfo")
                grantees = []
                for user_obj in listing["users"]:
                    grantees.append(
                        {"user": user_obj["user"], "roles": user_obj["roles"]}.__str__()
                    )
                row = {
                    "db_name": db_name,
                    "grantees": grantees,
                    "saved": False,
                }
                rows.append(row)
            query_result.rows = rows
        return query_result

    def get_instance_users_summary(self):
        """Instance account management: get summary for all users."""
        query_result = self.get_all_databases()
        if not query_result.error:
            dbs = query_result.rows
            conn = self.get_connection()

            # Get database user info.
            rows = []
            for db_name in dbs:
                # Execute command.
                listing = conn[db_name].command(command="usersInfo")
                for user_obj in listing["users"]:
                    rows.append(
                        {
                            "db_name_user": f"{db_name}.{user_obj['user']}",
                            "db_name": db_name,
                            "user": user_obj["user"],
                            "roles": [role["role"] for role in user_obj["roles"]],
                            "saved": False,
                        }
                    )
            query_result.rows = rows
        return query_result

    def create_instance_user(self, **kwargs):
        """Instance account management: create account."""
        exec_result = ResultSet()
        db_name = kwargs.get("db_name", "")
        user = kwargs.get("user", "")
        password1 = kwargs.get("password1", "")
        remark = kwargs.get("remark", "")
        try:
            conn = self.get_connection()
            conn[db_name].command("createUser", user, pwd=password1, roles=[])
            exec_result.rows = [
                {
                    "instance": self.instance,
                    "db_name": db_name,
                    "user": user,
                    "password": password1,
                    "remark": remark,
                }
            ]
        except Exception as e:
            exec_result.error = str(e)
        return exec_result

    def drop_instance_user(self, db_name_user: str, **kwarg):
        """Instance account management: drop account."""
        arr = db_name_user.split(".")
        db_name = arr[0]
        user = arr[1]
        exec_result = ResultSet()
        try:
            conn = self.get_connection()
            conn[db_name].command("dropUser", user)
        except Exception as e:
            exec_result.error = str(e)
        return exec_result

    def reset_instance_user_pwd(self, db_name_user: str, reset_pwd: str, **kwargs):
        """Instance account management: reset account password."""
        arr = db_name_user.split(".")
        db_name = arr[0]
        user = arr[1]
        exec_result = ResultSet()
        try:
            conn = self.get_connection()
            conn[db_name].command("updateUser", user, pwd=reset_pwd)
        except Exception as e:
            exec_result.error = str(e)
        return exec_result

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Given SQL, DB name and result set, return masked result set."""
        mask_result = data_masking(self.instance, db_name, sql, resultset)
        return mask_result
