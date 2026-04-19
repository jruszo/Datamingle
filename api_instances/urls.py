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
    path("v1/instance/tunnel/", views.TunnelList.as_view()),
    path("v1/instance/rds/", views.AliyunRdsList.as_view()),
]
