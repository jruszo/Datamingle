from django.urls import include, path
from common import views

urlpatterns = [
    path("api/", include(("sql_api.urls", "sql_api"), namespace="sql_api")),
]

handler400 = views.bad_request
handler403 = views.permission_denied
handler404 = views.page_not_found
handler500 = views.server_error
