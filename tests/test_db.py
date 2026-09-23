import unittest

from db import sqlalchemy_url


class SqlalchemyUrlTests(unittest.TestCase):
    def test_uses_the_psycopg_driver(self):
        self.assertEqual(
            sqlalchemy_url("postgresql://user:pass@db/name"),
            "postgresql+psycopg://user:pass@db/name",
        )


if __name__ == "__main__":
    unittest.main()
