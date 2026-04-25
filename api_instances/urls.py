from django.urls import path

from api_instances import views

urlpatterns = [
    path("v1/instance/", views.InstanceList.as_view()),
    path("v1/instance/tag/", views.InstanceTagList.as_view()),
    path("v1/instance/tag/<int:pk>/", views.InstanceTagDetail.as_view()),
    path("v1/instance/metadata/", views.InstanceMetadata.as_view()),
    path("v1/instance/test-connection/", views.InstanceDraftConnectionTest.as_view()),
    path(
        "v1/instance/<int:pk>/test-connection/",
        views.InstanceConnectionTest.as_view(),
    ),
    path("v1/instance/<int:pk>/", views.InstanceDetail.as_view()),
    path("v1/instance/resource/", views.InstanceResource.as_view()),
    path(
        "v1/instance/data-dictionary/instances/",
        views.DataDictionaryInstanceList.as_view(),
    ),
    path(
        "v1/instance/data-dictionary/databases/",
        views.DataDictionaryDatabaseList.as_view(),
    ),
    path(
        "v1/instance/data-dictionary/tables/",
        views.DataDictionaryTableList.as_view(),
    ),
    path(
        "v1/instance/data-dictionary/table/",
        views.DataDictionaryTableDetail.as_view(),
    ),
    path(
        "v1/instance/data-dictionary/export/",
        views.DataDictionaryExport.as_view(),
    ),
    path(
        "v1/instance-operations/database/",
        views.InstanceOperationDatabaseListCreate.as_view(),
    ),
    path(
        "v1/instance-operations/database/instances/",
        views.InstanceOperationDatabaseInstanceList.as_view(),
    ),
    path(
        "v1/instance-operations/database/metadata/",
        views.InstanceOperationDatabaseDetail.as_view(),
    ),
    path(
        "v1/instance-operations/account/",
        views.InstanceOperationAccountListCreate.as_view(),
    ),
    path(
        "v1/instance-operations/account/instances/",
        views.InstanceOperationAccountInstanceList.as_view(),
    ),
    path(
        "v1/instance-operations/account/metadata/",
        views.InstanceOperationAccountMetadata.as_view(),
    ),
    path(
        "v1/instance-operations/account/password/",
        views.InstanceOperationAccountPassword.as_view(),
    ),
    path(
        "v1/instance-operations/account/lock/",
        views.InstanceOperationAccountLock.as_view(),
    ),
    path(
        "v1/instance-operations/account/delete/",
        views.InstanceOperationAccountDelete.as_view(),
    ),
    path(
        "v1/instance-operations/account/grant/",
        views.InstanceOperationAccountGrant.as_view(),
    ),
    path(
        "v1/instance-operations/param/",
        views.InstanceOperationParamList.as_view(),
    ),
    path(
        "v1/instance-operations/param/instances/",
        views.InstanceOperationParamInstanceList.as_view(),
    ),
    path(
        "v1/instance-operations/param/history/",
        views.InstanceOperationParamHistory.as_view(),
    ),
    path(
        "v1/instance-operations/param/edit/",
        views.InstanceOperationParamEdit.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/instances/",
        views.InstanceOperationDiagnosticInstanceList.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/processes/",
        views.InstanceOperationDiagnosticProcessList.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/kill/preview/",
        views.InstanceOperationDiagnosticKillPreview.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/kill/",
        views.InstanceOperationDiagnosticKill.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/tablespace/",
        views.InstanceOperationDiagnosticTablespace.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/transactions/",
        views.InstanceOperationDiagnosticTransactions.as_view(),
    ),
    path(
        "v1/instance-operations/diagnostic/locks/",
        views.InstanceOperationDiagnosticLocks.as_view(),
    ),
]
