import unittest

from app import create_app


class AuthFlowTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.client = self.app.test_client()

    def test_root_redirects_to_login_when_unauthenticated(self):
        response = self.client.get("/", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers.get("Location", ""))

    def test_api_returns_401_when_unauthenticated(self):
        response = self.client.get("/api/options")
        self.assertEqual(response.status_code, 401)
        payload = response.get_json() or {}
        self.assertEqual(payload.get("redirect_to"), "/login")

    def test_login_success_allows_access_to_root(self):
        login = self.client.post(
            "/api/auth/login",
            json={"username": "matheus", "password": "123456"},
        )
        self.assertEqual(login.status_code, 200)

        root = self.client.get("/")
        self.assertEqual(root.status_code, 200)

    def test_login_failure_returns_401(self):
        login = self.client.post(
            "/api/auth/login",
            json={"username": "matheus", "password": "senha_errada"},
        )
        self.assertEqual(login.status_code, 401)


if __name__ == "__main__":
    unittest.main()
