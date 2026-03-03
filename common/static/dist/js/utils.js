var onLoadErrorCallback = function (status, jqXHR) {
    if (status === 403) {
        alert("Permission error: you do not have permission to view this data!");
    } else if (jqXHR.statusText === "abort" || jqXHR.statusText === "canceled"){
        return 0
    } else {
        alert("Unknown error, please contact the administrator!");
    }
};

var dateFormat = function(fmt, date) {
    var o = {
        "M+": date.getMonth() + 1,                    // month
        "d+": date.getDate(),                         // day
        "h+": date.getHours(),                        // hour
        "m+": date.getMinutes(),                      // minute
        "s+": date.getSeconds(),                      // second
        "q+": Math.floor((date.getMonth() + 3) / 3), // quarter
        "S": date.getMilliseconds()                   // millisecond
    };
    if(/(y+)/.test(fmt))
        fmt = fmt.replace(RegExp.$1, (date.getFullYear() + "").substr(4 - RegExp.$1.length));
    for(var k in o)
        if(new RegExp("(" + k + ")").test(fmt))
            fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
};

// Format and highlight JSON strings
var jsonHighLight = function(json) {
    json = json.toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'text-muted';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'text-success';
            } else {
                match = match
                cls = 'text-primary';
            }
        } else if (/true|false/.test(match)) {
            cls = 'text-success';
        } else if (/null/.test(match)) {
            cls = 'text-warning';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
};

// On the instance config page, show/hide the mode field by db_type (mode only applies to redis)
(function($) {
    $(function() {
        let db_type = $('#id_db_type');
        let mode = $('#id_mode').parent().parent();

        function toggleMode(value) {
            value === 'redis' ? mode.show() : mode.hide();
        }

        toggleMode(db_type.val());

        db_type.change(function() {
            toggleMode($(this).val());
        });
    });
})(jQuery);

// SMS verification code countdown
let countdown = 60, clock, btn_captcha;
function init_countdown(obj) {
    btn_captcha = obj;
    btn_captcha.disabled = true;
    btn_captcha.innerText = countdown + 's to retry';
    clock = setInterval(countdown_loop, 1000);
}

function countdown_loop() {
    countdown--;
    if(countdown > 0){
        btn_captcha.innerText = countdown + 's to retry';
    } else {
        clearInterval(clock);
        btn_captcha.disabled = false;
        btn_captcha.innerText = 'Get verification code';
        countdown = 60;
    }
}
