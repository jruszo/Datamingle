from unittest.mock import patch, MagicMock

# ========================================================
# Step 1: Prepare the patcher and keep a reference for cleanup
# ========================================================
requests_patcher = patch("requests.get")

mock_response = MagicMock()
mock_response.status_code = 200
mock_response.json.return_value = {
    "authorization_endpoint": "https://example.com/auth",
    "token_endpoint": "https://example.com/token",
    "userinfo_endpoint": "https://example.com/userinfo",
    "jwks_uri": "https://example.com/jwks",
    "end_session_endpoint": "https://example.com/logout",
}

# Start patching immediately to protect subsequent Django imports
mock_get = requests_patcher.start()
mock_get.return_value = mock_response

# ========================================================
# Step 2: Import Django components after patch setup
# ========================================================
from django.test import TestCase, override_settings
from django.core.exceptions import SuspiciousOperation
from django.contrib.auth import get_user_model
from django.conf import settings

# Keep common imports after this point because they usually import settings internally
from common.authenticate.oidc_auth import OIDCAuthenticationBackend

# Get User model
User = get_user_model()

# ========================================================
# Step 3: Define test cases
# ========================================================
OIDC_MOCK_ENDPOINTS = {
    "OIDC_OP_AUTHORIZATION_ENDPOINT": "https://example.com/auth",
    "OIDC_OP_TOKEN_ENDPOINT": "https://example.com/token",
    "OIDC_OP_USER_ENDPOINT": "https://example.com/userinfo",
    "OIDC_OP_JWKS_ENDPOINT": "https://example.com/jwks",
    "OIDC_OP_LOGOUT_ENDPOINT": "https://example.com/logout",
    "OIDC_RP_CLIENT_ID": "exampleoidcrpclientid",
    "OIDC_RP_CLIENT_SECRET": "exampleoidcrpclientsecret",
}


@override_settings(**OIDC_MOCK_ENDPOINTS)
class OIDCAuthTest(TestCase):
    @classmethod
    def setUpClass(cls):
        # Do not call patch().start() again; the global patch is already active.
        # Reuse global patcher and mock objects directly.
        cls.requests_patcher = requests_patcher
        cls.mock_get = mock_get

        # Call original setUpClass (triggers Django settings handling)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        # Stop global patch after tests to avoid leaking into other test files
        super().tearDownClass()
        cls.requests_patcher.stop()

    def setUp(self):
        self.backend = OIDCAuthenticationBackend()
        self.claims = {
            "email": "test@example.com",
            "preferred_username": "testuser",
            "name": "Test User",
        }

    def test_default_attribute_mapping(self):
        """Test that the default attribute mapping is loaded from settings."""
        attr_map = self.backend._get_oidc_attr_map()
        expected_map = getattr(settings, "OIDC_USER_ATTR_MAP", {})
        self.assertEqual(attr_map, expected_map)

    def test_create_user_success(self):
        """Test creating a user with standard claims."""
        user = self.backend.create_user(self.claims)
        self.assertEqual(user.username, "testuser")
        self.assertEqual(user.email, "test@example.com")
        self.assertEqual(user.display, "Test User")
        self.assertTrue(User.objects.filter(username="testuser").exists())

    @override_settings(
        OIDC_USER_ATTR_MAP={"username": "sub", "email": "mail", "display": "nickname"}
    )
    def test_create_user_custom_mapping(self):
        """Test creating a user with custom attribute mapping."""
        custom_claims = {
            "mail": "custom@example.com",
            "sub": "customuser",
            "nickname": "Custom User",
        }
        user = self.backend.create_user(custom_claims)
        self.assertEqual(user.username, "customuser")
        self.assertEqual(user.email, "custom@example.com")
        self.assertEqual(user.display, "Custom User")

    @override_settings(
        OIDC_USER_ATTR_MAP="username=preferred_username,display=name,email=email"
    )
    def test_create_user_string_config(self):
        """Test creating a user with string configuration for attribute mapping."""
        user = self.backend.create_user(self.claims)
        self.assertEqual(user.username, "testuser")
        self.assertEqual(user.email, "test@example.com")
        self.assertEqual(user.display, "Test User")

    def test_create_user_missing_email(self):
        """Test that SuspiciousOperation is raised when email is missing."""
        claims = self.claims.copy()
        del claims["email"]
        with self.assertRaises(SuspiciousOperation) as cm:
            self.backend.create_user(claims)
        self.assertIn("Missing OIDC fields: email", str(cm.exception))

    def test_create_user_missing_username(self):
        """Test that SuspiciousOperation is raised when username is missing."""
        claims = self.claims.copy()
        del claims["preferred_username"]
        with self.assertRaises(SuspiciousOperation) as cm:
            self.backend.create_user(claims)
        self.assertIn("Missing OIDC fields: preferred_username", str(cm.exception))

    def test_create_user_missing_display(self):
        """Test that SuspiciousOperation is raised when display name is missing."""
        claims = self.claims.copy()
        del claims["name"]
        with self.assertRaises(SuspiciousOperation) as cm:
            self.backend.create_user(claims)
        self.assertIn("Missing OIDC fields: name", str(cm.exception))

    def test_describe_user_by_claims(self):
        """Test describing user by claims."""
        description = self.backend.describe_user_by_claims(self.claims)
        self.assertEqual(description, "username testuser")

    def test_filter_users_by_claims_success(self):
        """Test filtering users finds an existing user."""
        User.objects.create_user(username="testuser", email="test@example.com")
        users = self.backend.filter_users_by_claims(self.claims)
        self.assertEqual(users.count(), 1)
        self.assertEqual(users.first().username, "testuser")

    def test_filter_users_by_claims_not_found(self):
        """Test filtering users returns empty if user doesn't exist."""
        users = self.backend.filter_users_by_claims(self.claims)
        self.assertEqual(users.count(), 0)

    def test_filter_users_by_claims_admin_block(self):
        """Test that the 'admin' username is blocked."""
        claims = {"preferred_username": "admin"}
        # Ensure 'admin' user exists so we know we aren't just getting empty because of no user
        User.objects.create_user(username="admin", email="admin@example.com")
        users = self.backend.filter_users_by_claims(claims)
        self.assertEqual(users.count(), 0)
