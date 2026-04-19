from django.urls import path

from api_archives import views

urlpatterns = [
    path("v1/archive/metadata/", views.ArchiveMetadata.as_view()),
    path("v1/archive/approval-preview/", views.ArchiveApprovalPreview.as_view()),
    path("v1/archive/", views.ArchiveListCreate.as_view()),
    path("v1/archive/<int:archive_id>/", views.ArchiveDetail.as_view()),
    path(
        "v1/archive/<int:archive_id>/reviews/",
        views.ArchiveReviewCreate.as_view(),
    ),
    path("v1/archive/<int:archive_id>/run/", views.ArchiveRunNow.as_view()),
    path("v1/archive/<int:archive_id>/state/", views.ArchiveStateUpdate.as_view()),
    path("v1/archive/<int:archive_id>/logs/", views.ArchiveLogList.as_view()),
]
