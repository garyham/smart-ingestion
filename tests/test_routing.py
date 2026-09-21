import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from ingestion.routing import route_document
from ingestion.tika import TikaDocument


class RoutingTests(unittest.TestCase):
    @patch("ingestion.routing.concurrency", return_value=nullcontext())
    @patch("ingestion.routing.tika_ingest_flow")
    @patch("ingestion.routing.xlsx_ingest_flow")
    @patch("ingestion.routing.extract_with_tika")
    def test_uses_tika_for_a_normal_document(
        self, extract, xlsx_flow, tika_flow, _concurrency
    ):
        result = TikaDocument("application/pdf", {}, "content")
        extract.return_value = result

        route_document(Path("report.pdf"), Path("output"))

        tika_flow.assert_called_once_with(Path("report.pdf"), result, Path("output"))
        xlsx_flow.assert_not_called()

    @patch("ingestion.routing.concurrency", return_value=nullcontext())
    @patch("ingestion.routing.tika_ingest_flow")
    @patch("ingestion.routing.xlsx_ingest_flow")
    @patch("ingestion.routing.extract_with_tika")
    def test_adds_duckdb_processing_for_excel(
        self, extract, xlsx_flow, tika_flow, _concurrency
    ):
        result = TikaDocument(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            {},
            "content",
        )
        extract.return_value = result

        route_document(Path("report.xlsx"), Path("output"))

        xlsx_flow.assert_called_once_with(Path("report.xlsx"), result, Path("output"))
        tika_flow.assert_not_called()


if __name__ == "__main__":
    unittest.main()
