const pgsqlDiagnosticInfo = {
    fieldsProcesslist: [
        'pgsql',
        ["All", "Not Idle"],
        [
            { title: '', field: 'checkbox', checkbox: true },
            { title: 'PId', field: 'pid', sortable: true },
            { title: 'Blocking PID', field: 'block_pids', sortable: false },
            { title: 'Database', field: 'datname', sortable: true },
            { title: 'User', field: 'usename', sortable: true },
            { title: 'Application Name', field: 'application_name', sortable: true },
            { title: 'Status', field: 'state', sortable: true },
            { title: 'Client Address', field: 'client_addr', sortable: true },
            { title: 'Elapsed Time (s)', field: 'elapsed_time_seconds', sortable: true },
            { title: 'Elapsed Time', field: 'elapsed_time', sortable: true },
            { title: 'Query', field: 'query', sortable: true },
            { title: 'Wait Event Type', field: 'wait_event_type', sortable: true },
            { title: 'Wait Event', field: 'wait_event', sortable: true },
            { title: 'Query Start Time', field: 'query_start', sortable: true },
            { title: 'Backend Start Time', field: 'backend_start', sortable: true },
            { title: 'Parent PID', field: 'leader_pid', sortable: true },
            { title: 'Client Hostname', field: 'client_hostname', sortable: true },
            { title: 'Client Port', field: 'client_port', sortable: true },
            { title: 'Transaction Start Time', field: 'transaction_start_time', sortable: true },
            { title: 'State Change Time', field: 'state_change', sortable: true },
            { title: 'Backend XID', field: 'backend_xid', sortable: true },
            { title: 'Backend XMIN', field: 'backend_xmin', sortable: true },
            { title: 'Backend Type', field: 'backend_type', sortable: true },
        ]
    ],

}

const mysqlDiagnosticInfo = {
    fieldsProcesslist: [
        'mysql',
        ["All", "Not Sleep", "Query"],
        [{
            title: '',
            field: 'checkbox',
            checkbox: true
        }, {
            title: 'THEEAD ID',
            field: 'id',
            sortable: true
        }, {
            title: 'USER',
            field: 'user',
            sortable: true
        }, {
            title: 'HOST',
            field: 'host',
            sortable: true
        }, {
            title: 'DATABASE',
            field: 'db',
            sortable: true
        }, {
            title: 'TIME(s)',
            field: 'time',
            sortable: true
        }, {
            title: 'COMMAND',
            field: 'command',
            sortable: true
        }, {
            title: 'STATE',
            field: 'state',
            sortable: true
        }, {
            title: 'INFO',
            field: 'info',
            sortable: true,
            formatter: function (value, row, index) {
                if (value.length > 20) {
                    var sql = value.substr(0, 20) + '...';
                    return sql;
                } else {
                    return value
                }
            }
        }, {
            title: 'Full INFO',
            field: 'info',
            sortable: true,
            visible: false // hidden by default
        }],
        function (index, row) {
            var html = [];
            $.each(row, function (key, value) {
                if (key === 'info') {
                    var sql = window.sqlFormatter.format(value);
                    // Replace all newline characters
                    sql = sql.replace(/\r\n/g, "<br>");
                    sql = sql.replace(/\n/g, "<br>");
                    // Replace all spaces
                    sql = sql.replace(/\s/g, "&nbsp;");
                    html.push('<span>' + sql + '</span>');
                }
            });
            return html.join('');
        }
    ],
}

const dorisDiagnosticInfo = {
    fieldsProcesslist: [
        'doris',
        ["All","Not Sleep","Query"],
        [{
            title: '',
            field: 'checkbox',
            checkbox: true
        }, {
            title: 'THEEAD ID',
            field: 'id',
            sortable: true
        }, {
            title: 'USER',
            field: 'user',
            sortable: true
        }, {
            title: 'HOST',
            field: 'host',
            sortable: true
        }, {
            title: 'CATALOG',
            field: 'catalog',
            sortable: true
        }, {
            title: 'DATABASE',
            field: 'db',
            sortable: true
        }, {
            title: 'TIME(s)',
            field: 'time',
            sortable: true
        }, {
            title: 'COMMAND',
            field: 'command',
            sortable: true
        }, {
            title: 'STATE',
            field: 'state',
            sortable: true
        }, {
            title: 'INFO',
            field: 'info',
            sortable: true,
            formatter: function (value, row, index) {
                if (value.length > 20) {
                    var sql = value.substr(0, 20) + '...';
                    return sql;
                } else {
                    return value
                }
            }
        }, {
            title: 'QUERYID',
            field: 'query_id',
            sortable: true,
            visible: false // hidden by default
        }, {
            title: 'Full INFO',
            field: 'info',
            sortable: true,
            visible: false // hidden by default
        }, {
            title: 'FE',
            field: 'fe',
            sortable: true,
            visible: false // hidden by default
        }],
        function (index, row) {
            var html = [];
            $.each(row, function (key, value) {
                if (key === 'info') {
                    var sql = window.sqlFormatter.format(value);
                    // Replace all newline characters
                    sql = sql.replace(/\r\n/g, "<br>");
                    sql = sql.replace(/\n/g, "<br>");
                    // Replace all spaces
                    sql = sql.replace(/\s/g, "&nbsp;");
                    html.push('<span>' + sql + '</span>');
                }
            });
            return html.join('');
        }
    ]
}

const mongoDiagnosticInfo = {
    fieldsProcesslist: [
        'mongo',
        ["All", "Active", "Full", "Inner"],
        [{
            title: '',
            field: 'checkbox',
            checkbox: true
        }, {
            title: 'opid',
            field: 'opid',
            sortable: true
        }, {
            title: 'client',
            field: 'client',
            sortable: true
        }, {
            title: 'client_s',
            field: 'client_s',
            sortable: true
        }, {
            title: 'type',
            field: 'type',
            sortable: true
        }, {
            title: 'active',
            field: 'active',
            sortable: true
        }, {
            title: 'desc',
            field: 'desc',
            sortable: true
        }, {
            title: 'ns',
            field: 'ns',
            sortable: true
        }, {
            title: 'effectiveUsers_user',
            field: 'effectiveUsers_user',
            sortable: true
        }
            , {
            title: 'secs_running',
            field: 'secs_running',
            sortable: true
        }
            , {
            title: 'microsecs_running',
            field: 'microsecs_running',
            sortable: true
        }, {
            title: 'waitingForLock',
            field: 'waitingForLock',
            sortable: true

        }, {
            title: 'locks',
            field: 'locks',
            sortable: true,
            formatter: function (value, row, index) {
                return JSON.stringify(value);
            },
            visible: false
        }, {
            title: 'lockStats',
            field: 'lockStats',
            sortable: true,
            formatter: function (value, row, index) {
                return JSON.stringify(value);
            },
            visible: false
        }, {
            title: 'command',
            field: 'command',
            sortable: true,
            formatter: function (value, row, index) {
                if (value) {
                    let c = JSON.stringify(value);
                    if (c.length > 20) {
                        return c.substr(0, 80) + '...}';
                    } else {
                        return c;
                    }
                }
            }
        }, {
            title: 'Full command',
            field: 'command',
            sortable: true,
            formatter: function (value, row, index) {
                return JSON.stringify(value);
            },
            visible: false // hidden by default
        }, {
            title: 'clientMetadata',
            field: 'clientMetadata',
            sortable: true,
            formatter: function (value, row, index) {
                return JSON.stringify(value);
            },
            visible: false // hidden by default
        }],
        function (index, row) {
            delete row['checkbox'];
            return "<pre>" + jsonHighLight(JSON.stringify(row, null, 2)) + "</pre>";
        }
    ],
}

const redisDiagnosticInfo = {
    fieldsProcesslist: [
        'redis',
        ["All"],
        [{
            title: '',  // for multi-select checkbox
            field: 'checkbox',
            checkbox: true
        }, {
            title: 'Id',
            field: 'id',
            sortable: true
        }, {
            title: 'Remote Address',
            field: 'addr',
            sortable: true
        }, {
            title: 'Local Address',
            field: 'laddr',
            sortable: true
        }, {
            title: 'Client Name',
            field: 'name',
            sortable: true
        }, {
            title: 'User',
            field: 'user',
            sortable: true
        },
        {
            title: 'Database',
            field: 'db',
            sortable: true
        }, {
            title: 'Connection Age (s)',
            field: 'age',
            sortable: true
        }, {
            title: 'Idle Time (s)',
            field: 'idle',
            sortable: true
        }, {
            title: 'Command',
            field: 'cmd',
            sortable: true
        }, {
            title: 'Total Memory',
            field: 'tot-mem',
            sortable: true
        }, {
            title: 'Output Memory',
            field: 'omem',
            sortable: true
        }, {
            title: 'Flags',
            field: 'flags',
            sortable: true
        }, {
            title: 'File Descriptor',
            field: 'fd',
            sortable: true
        }, {
            title: 'Subscription Count',
            field: 'sub',
            sortable: true
        }, {
            title: 'Pattern Subscription Count',
            field: 'psub',
            sortable: true
        }, {
            title: 'MULTI Queue Length',
            field: 'multi',
            sortable: true
        }, {
            title: 'Query Buffer',
            field: 'qbuf',
            sortable: true
        }, {
            title: 'Query Buffer Free',
            field: 'qbuf-free',
            sortable: true
        }, {
            title: 'Argument Memory',
            field: 'argv-mem',
            sortable: true
        }, {
            title: 'Output Buffer Length',
            field: 'obl',
            sortable: true
        }, {
            title: 'Output List Length',
            field: 'oll',
            sortable: true
        }, {
            title: 'Events',
            field: 'events',
            sortable: true
        }, {
            title: 'Redirect',
            field: 'redir',
            sortable: true
        }],
        function (index, row) {
            var html = [];
        }
    ],
}
const oracleDiagnosticInfo = {
    fieldsProcesslist: [
        'oracle',
        ["All", "Active", "Others"],
        [{
            title: '',
            field: 'checkbox',
            checkbox: true
        }, {
            title: 'SESSION ID',
            field: 'SID',
            sortable: true
        }, {
            title: 'SERIAL#',
            field: 'SERIAL#',
            sortable: true
        }, {
            title: 'STATUS',
            field: 'STATUS',
            sortable: true
        }, {
            title: 'USER',
            field: 'USERNAME',
            sortable: true
        }, {
            title: 'MACHINE',
            field: 'MACHINE',
            sortable: true
        }, {
            title: 'SQL',
            field: 'SQL_TEXT',
            sortable: true,
            formatter: function (value, row, index) {
                if (row.SQL_TEXT.length > 60) {
                    let sql = row.SQL_TEXT.substr(0, 60) + '...';
                    return sql;
                } else {
                    return value
                }
            }
        }, {
            title: 'FULL SQL',
            field: 'SQL_FULLTEXT',
            visible: false,
            sortable: true
        }, {
            title: 'START TIME',
            field: 'SQL_EXEC_START',
            sortable: true
        }],
        function (index, row) {
            var html = [];
            $.each(row, function (key, value) {
                if (key === 'SQL_FULLTEXT') {
                    var sql = window.sqlFormatter.format(value);
                    // Replace all newline characters
                    sql = sql.replace(/\r\n/g, "<br>");
                    sql = sql.replace(/\n/g, "<br>");
                    // Replace all spaces
                    sql = sql.replace(/\s/g, "&nbsp;");
                    html.push('<span>' + sql + '</span>');
                }
            });
            return html.join('');
        }
    ],
}
