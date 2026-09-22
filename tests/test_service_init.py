import unittest
from unittest.mock import patch

from service_init import initialize


class ServiceInitTests(unittest.TestCase):
    @patch("service_init.ensure_concurrency_limits")
    @patch("service_init.load_config")
    @patch("service_init.ensure_schema")
    def test_initializes_schema_then_prefect_limits(
        self, ensure_schema, load_config, ensure_limits
    ):
        load_config.return_value = {"concurrency_limits": {"tika": 4}}

        initialize()

        ensure_schema.assert_called_once_with()
        ensure_limits.assert_called_once_with({"tika": 4})


if __name__ == "__main__":
    unittest.main()
