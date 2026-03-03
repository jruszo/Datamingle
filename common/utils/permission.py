# -*- coding: UTF-8 -*-
import simplejson as json
from django.shortcuts import render
from django.http import HttpResponse


# Permission check for admin-only operations
def superuser_required(func):
    def wrapper(request, *args, **kw):
        # Read user info and verify permission
        user = request.user

        if user.is_superuser is False:
            is_ajax = request.META.get("HTTP_X_REQUESTED_WITH") == "XMLHttpRequest"
            if is_ajax:
                result = {"status": 1, "msg": "You are not authorized. Contact admin.", "data": []}
                return HttpResponse(json.dumps(result), content_type="application/json")
            else:
                context = {"errMsg": "You are not authorized. Contact admin."}
                return render(request, "error.html", context)

        return func(request, *args, **kw)

    return wrapper


# Permission check for role-based operations
def role_required(roles=()):
    def _deco(func):
        def wrapper(request, *args, **kw):
            # Read user info and verify role
            user = request.user
            if user.role not in roles and user.is_superuser is False:
                is_ajax = request.META.get("HTTP_X_REQUESTED_WITH") == "XMLHttpRequest"
                if is_ajax:
                    result = {
                        "status": 1,
                        "msg": "You are not authorized. Contact admin.",
                        "data": [],
                    }
                    return HttpResponse(
                        json.dumps(result), content_type="application/json"
                    )
                else:
                    context = {"errMsg": "You are not authorized. Contact admin."}
                    return render(request, "error.html", context)

            return func(request, *args, **kw)

        return wrapper

    return _deco
