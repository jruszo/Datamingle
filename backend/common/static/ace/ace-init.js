// Initialize the ACE editor object
var editor = ace.edit("sql_content_editor");
ace.config.set('basePath', '/static/ace');
ace.config.set('modePath', '/static/ace');
ace.config.set('themePath', '/static/ace');

// Set theme and language (for more options, check the ACE GitHub directories)
var theme = "textmate";
var language = "text";
editor.setTheme("ace/theme/" + theme);
editor.session.setMode("ace/mode/" + language);
editor.$blockScrolling = Infinity;
editor.setValue("");

// Font size
editor.setFontSize(12);

// Set read-only mode (true means read-only, used for code display)
editor.setReadOnly(false);

// Enable word wrap; set to off to disable
editor.setOption("wrap", "free");
editor.getSession().setUseWrapMode(true);

// Enable autocomplete menu
ace.require("ace/ext/language_tools");
editor.setOptions({
    enableBasicAutocompletion: true,
    enableSnippets: true,
    enableLiveAutocompletion: true
});

// Enable search extension
ace.require("ace/ext/language_tools");

// Bind query shortcut
editor.commands.addCommand({
    name: "alter",
    bindKey: {win: "Ctrl-Enter", mac: "Command-Enter"},
    exec: function (editor) {
        let pathname = window.location.pathname;
        if (pathname === "/sqlquery/") {
            dosqlquery()
        }
    }
});

// Set autocomplete entries
var setCompleteData = function (data) {
    var langTools = ace.require("ace/ext/language_tools");
    langTools.addCompleter({
        getCompletions: function (editor, session, pos, prefix, callback) {
            if (prefix.length === 0) {
                return callback(null, []);
            } else {
                return callback(null, data);
            }
        }
    });
};

// Add database suggestions
function setDbsCompleteData(result) {
    var tables = [];
    for (var i = 0; i < result.length; i++) {
        tables.push({
            name: result[i],
            value: result[i],
            caption: result[i],
            meta: "database",
            score: 100
        });

    }
    setCompleteData(tables);
}

// Add schema suggestions
function setSchemasCompleteData(result) {
    var tables = [];
    for (var i = 0; i < result.length; i++) {
        tables.push({
            name: result[i],
            value: result[i],
            caption: result[i],
            meta: "schema",
            score: 100
        });

    }
    setCompleteData(tables);
}


// Add table suggestions
function setTablesCompleteData(result) {
    var meta = $("#db_name").val();
    if ($("#schema_name").val()) {
        meta = $("#schema_name").val();
    }
    var tables = [];
    for (var i = 0; i < result.length; i++) {
        tables.push({
            name: result[i],
            value: result[i],
            caption: result[i],
            meta: meta,
            score: 100
        });

    }
    setCompleteData(tables);
}

// Add column suggestions
function setColumnsCompleteData(result) {
    if (result) {
        var tables = [];
        for (var i = 0; i < result.length; i++) {
            tables.push({
                name: result[i],
                value: result[i],
                caption: result[i],
                meta: $("#table_name").val(),
                score: 100
            });

        }
        setCompleteData(columns);
    } else {
        $.ajax({
            type: "get",
            url: "/instance/instance_resource/",
            dataType: "json",
            data: {
                instance_name: $("#instance_name").val(),
                db_name: $("#db_name").val(),
                schema_name: $("#schema_name").val(),
                tb_name: $("#table_name").val(),
                resource_type: "column"
            },
            complete: function () {
            },
            success: function (data) {
                if (data.status === 0) {
                    var result = data.data;
                    var columns = [];
                    for (var i = 0; i < result.length; i++) {
                        columns.push({
                            name: result[i],
                            value: result[i],
                            caption: result[i],
                            meta: $("#table_name").val(),
                            score: 100
                        })
                    }
                    setCompleteData(columns);
                } else {
                    alert(data.msg);
                }
            }
        });
    }
}

// Update language when instance changes
$("#instance_name").change(function () {
    let optgroup = $('#instance_name :selected').parent().attr('label');
    if (optgroup === "MySQL") {
        editor.setTheme("ace/theme/" + "textmate");
        editor.session.setMode("ace/mode/" + "mysql");
        // Prompt text
        let pathname = window.location.pathname;
        if (pathname === "/submitsql/" && !editor.getValue()) {
            editor.setValue("-- Please enter SQL here. End with a semicolon. Only DML and DDL statements are supported. Use SQL Query for query statements.\n");
            editor.clearSelection();
            editor.focus();  // Focus editor
        }
    } else if (optgroup === "MsSQL") {
        editor.setTheme("ace/theme/" + "sqlserver");
        editor.session.setMode("ace/mode/" + "sqlserver");
    } else if (optgroup === "Redis") {
        editor.setTheme("ace/theme/" + "textmate");
        editor.session.setMode("ace/mode/" + "text");
        editor.setOptions({
            enableSnippets: false,
        });
        // Prompt text
        let pathname = window.location.pathname;
        if (pathname === "/submitsql/" && !editor.getValue()) {
            editor.setValue("Please enter commands here. Put multiple commands on separate lines and remove this instruction line before submitting.");
            editor.focus();  // Focus editor
        }
    } else if (optgroup === "PgSQL") {
        editor.setTheme("ace/theme/" + "textmate");
        editor.session.setMode("ace/mode/" + "pgsql");
    } else if (optgroup === "Oracle") {
        editor.setTheme("ace/theme/" + "textmate");
        editor.session.setMode("ace/mode/" + "sql");
    } else if (optgroup === "Mongo") {
        editor.setTheme("ace/theme/" + "textmate");
        editor.session.setMode("ace/mode/" + "mongodb");
        editor.setOptions({
            enableSnippets: false,
        });
    } else {
        editor.setTheme("ace/theme/" + "textmate");
        editor.session.setMode("ace/mode/" + "mysql");
    }
});
