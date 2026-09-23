import unittest
from contextlib import nullcontext
from unittest.mock import patch

from ingestion.routing import CHUNKS, DUCKDB, concurrency_slot, route_mime_type

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class RoutingTests(unittest.TestCase):
    def test_a_normal_document_routes_to_the_chunking_path(self):
        self.assertEqual(route_mime_type("application/pdf"), CHUNKS)

    def test_a_spreadsheet_routes_to_its_own_path(self):
        self.assertEqual(route_mime_type(XLSX_MIME), DUCKDB)
        self.assertEqual(route_mime_type("application/vnd.ms-excel"), DUCKDB)

    @patch("ingestion.routing.concurrency", return_value=nullcontext())
    def test_a_slot_is_taken_from_the_named_limit(self, concurrency):
        with concurrency_slot("xlsx"):
            pass

        self.assertEqual(concurrency.call_args.args[0], "xlsx-ingest")
        self.assertTrue(concurrency.call_args.kwargs["strict"])


if __name__ == "__main__":
    unittest.main()
