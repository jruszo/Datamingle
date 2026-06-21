from allauth.account.models import EmailAddress
from django.contrib.auth.models import Group
from rest_framework import status
from rest_framework.test import APITestCase

from common.auth import SUPERADMIN_GROUP_NAME
from sql.models import Team, Users


def create_email_user(
    username,
    password="SecurePass123!",
    *,
    email=None,
    display="Test User",
    is_active=True,
    is_superuser=False,
):
    email = email or username
    user = Users.objects.create_user(
        username=username,
        email=email,
        password=password,
        display=display,
        is_active=is_active,
        is_superuser=is_superuser,
        is_staff=is_superuser,
    )
    EmailAddress.objects.create(
        user=user,
        email=email,
        primary=True,
        verified=True,
    )
    return user


class AllauthHeadlessAuthTests(APITestCase):
    def login(self, email="user@datamingle.dev", password="SecurePass123!"):
        response = self.client.post(
            "/api/_allauth/app/v1/auth/login",
            {"email": email, "password": password},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.json())
        meta = response.json()["meta"]
        return meta["access_token"], meta["refresh_token"]

    def test_email_password_login_refresh_and_current_user(self):
        create_email_user(
            username="user@datamingle.dev",
            email="user@datamingle.dev",
            display="Local User",
        )

        access_token, refresh_token = self.login()
        self.assertTrue(access_token)
        self.assertTrue(refresh_token)

        current_user_response = self.client.get(
            "/api/v1/me/",
            HTTP_AUTHORIZATION=f"Bearer {access_token}",
            format="json",
        )
        self.assertEqual(current_user_response.status_code, status.HTTP_200_OK)
        payload = current_user_response.json()["data"]
        self.assertEqual(payload["email"], "user@datamingle.dev")
        self.assertNotIn("is_workos_managed", payload)

        refresh_response = self.client.post(
            "/api/_allauth/app/v1/tokens/refresh",
            {"refresh_token": refresh_token},
            format="json",
        )
        self.assertEqual(refresh_response.status_code, status.HTTP_200_OK)
        self.assertTrue(refresh_response.json()["data"]["access_token"])

    def test_inactive_user_cannot_login(self):
        create_email_user(
            username="inactive@datamingle.dev",
            email="inactive@datamingle.dev",
            is_active=False,
        )

        response = self.client.post(
            "/api/_allauth/app/v1/auth/login",
            {"email": "inactive@datamingle.dev", "password": "SecurePass123!"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_signup_is_closed(self):
        response = self.client.post(
            "/api/_allauth/app/v1/auth/signup",
            {
                "email": "signup@datamingle.dev",
                "password": "SecurePass123!",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class LocalUserManagementTests(APITestCase):
    def setUp(self):
        self.auth_group, _ = Group.objects.get_or_create(name=SUPERADMIN_GROUP_NAME)
        self.team = Team.objects.create(team_name="Primary Team")
        self.superuser = create_email_user(
            username="admin@datamingle.dev",
            email="admin@datamingle.dev",
            display="Admin User",
            is_superuser=True,
        )
        self.access_token = self.login_as_superuser()

    def tearDown(self):
        Users.objects.all().delete()
        Team.objects.all().delete()
        Group.objects.all().delete()

    def login_as_superuser(self):
        response = self.client.post(
            "/api/_allauth/app/v1/auth/login",
            {"email": "admin@datamingle.dev", "password": "SecurePass123!"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.json())
        return response.json()["meta"]["access_token"]

    def test_superuser_can_create_local_user(self):
        response = self.client.post(
            "/api/v1/user/",
            {
                "email": "operator@datamingle.dev",
                "display": "Operator",
                "password": "SecurePass123!",
            },
            HTTP_AUTHORIZATION=f"Bearer {self.access_token}",
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.json())
        payload = response.json()["data"]
        self.assertEqual(payload["username"], "operator@datamingle.dev")
        self.assertEqual(payload["email"], "operator@datamingle.dev")
        self.assertNotIn("is_workos_managed", payload)

        user = Users.objects.get(username="operator@datamingle.dev")
        self.assertTrue(user.check_password("SecurePass123!"))
        self.assertTrue(
            EmailAddress.objects.filter(
                user=user,
                email="operator@datamingle.dev",
                primary=True,
                verified=True,
            ).exists()
        )

    def test_create_local_user_rejects_duplicate_email(self):
        create_email_user(
            username="existing@datamingle.dev",
            email="existing@datamingle.dev",
        )

        response = self.client.post(
            "/api/v1/user/",
            {
                "email": "EXISTING@datamingle.dev",
                "password": "SecurePass123!",
            },
            HTTP_AUTHORIZATION=f"Bearer {self.access_token}",
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_non_superuser_cannot_create_local_user(self):
        create_email_user(
            username="regular@datamingle.dev",
            email="regular@datamingle.dev",
        )
        response = self.client.post(
            "/api/_allauth/app/v1/auth/login",
            {"email": "regular@datamingle.dev", "password": "SecurePass123!"},
            format="json",
        )
        token = response.json()["meta"]["access_token"]

        response = self.client.post(
            "/api/v1/user/",
            {
                "email": "blocked@datamingle.dev",
                "password": "SecurePass123!",
            },
            HTTP_AUTHORIZATION=f"Bearer {token}",
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
