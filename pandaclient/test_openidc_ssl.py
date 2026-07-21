"""Offline unit tests for openidc_utils._get_ssl_context.

These do not touch the network. They verify that an empty stdlib trust store
(e.g. a python.org macOS framework build) triggers a fall-back to the bundled
certifi roots, so OIDC login works without setting SSL_CERT_FILE.

Run with: python -m unittest pandaclient.test_openidc_ssl
"""

import ssl
import unittest
from unittest import mock

import certifi

from pandaclient.openidc_utils import _get_ssl_context


class TestGetSSLContext(unittest.TestCase):
    def test_returns_ssl_context(self):
        self.assertIsInstance(_get_ssl_context(), ssl.SSLContext)

    def test_empty_store_falls_back_to_certifi(self):
        # Simulate a python.org macOS build whose default store is empty.
        fake = mock.MagicMock()
        fake.get_ca_certs.return_value = []
        with mock.patch("ssl.create_default_context", return_value=fake):
            ctx = _get_ssl_context()
        self.assertIs(ctx, fake)
        fake.load_verify_locations.assert_called_once_with(cafile=certifi.where())

    def test_populated_store_does_not_load_certifi(self):
        # A working store must not be augmented with certifi.
        fake = mock.MagicMock()
        fake.get_ca_certs.return_value = [{"subject": ()}]
        with mock.patch("ssl.create_default_context", return_value=fake):
            ctx = _get_ssl_context()
        self.assertIs(ctx, fake)
        fake.load_verify_locations.assert_not_called()


if __name__ == "__main__":
    unittest.main()
