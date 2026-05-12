from django.urls import path

from api_auth import views

urlpatterns = [
    path(
        "auth/token/refresh/", views.SPATokenRefreshView.as_view(), name="token_refresh"
    ),
    path("auth/token/verify/", views.SPATokenVerifyView.as_view(), name="token_verify"),
    path(
        "auth/workos/authorize/",
        views.WorkOSAuthorizeView.as_view(),
        name="workos_authorize",
    ),
    path(
        "auth/workos/callback/",
        views.WorkOSCallbackView.as_view(),
        name="workos_callback",
    ),
    path(
        "auth/workos/exchange/",
        views.WorkOSExchangeView.as_view(),
        name="workos_exchange",
    ),
    path("auth/workos/logout/", views.WorkOSLogoutView.as_view(), name="workos_logout"),
]
