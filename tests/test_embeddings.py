import io
import json
import unittest
from unittest.mock import patch

import numpy as np
from fastembed import SparseEmbedding

from embeddings.flow import (
    PGVECTOR_MAX_SPARSE_DIMENSIONS,
    _normalise_sparse_vector,
    _retry_transient_store_error,
    _sparse_dimension,
    available_embedding_providers,
    load_chunks,
)
from embeddings.storage import (
    dense_vector_literal,
    hybrid_search,
    sparse_vector_literal,
)


class FakeS3Client:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


class EmbeddingFlowTests(unittest.TestCase):
    class FailedState:
        def __init__(self, failure):
            self.failure = failure

        def result(self, raise_on_failure=False):
            return self.failure

    @patch("embeddings.flow.s3_client")
    def test_load_chunks_uses_manifest_artifact(self, make_s3_client):
        manifest = {
            "artifacts": [
                {
                    "name": "chunks.jsonl",
                    "uri": "s3://smart-files/ingested/doc/run/chunks.jsonl",
                }
            ]
        }
        make_s3_client.return_value = FakeS3Client(
            {
                ("smart-files", "ingested/doc/run/manifest.json"): json.dumps(
                    manifest
                ).encode(),
                ("smart-files", "ingested/doc/run/chunks.jsonl"): (
                    b'{"index": 0, "text": "first"}\n{"index": 1, "text": "second"}\n'
                ),
            }
        )

        chunks = load_chunks.fn("s3://smart-files/ingested/doc/run/manifest.json")

        self.assertEqual([chunk["text"] for chunk in chunks], ["first", "second"])

    @patch(
        "embeddings.flow.ort.get_available_providers",
        return_value=["CUDAExecutionProvider", "CPUExecutionProvider"],
    )
    def test_auto_provider_prefers_cuda(self, _get_available_providers):
        self.assertEqual(
            available_embedding_providers(),
            ("CUDAExecutionProvider", "CPUExecutionProvider"),
        )

    @patch(
        "embeddings.flow.ort.get_available_providers",
        return_value=["CPUExecutionProvider"],
    )
    def test_requested_cuda_must_be_available(self, _get_available_providers):
        with self.assertRaises(RuntimeError):
            available_embedding_providers("cuda")

    def test_bm42_uses_pgvector_maximum_dimension(self):
        self.assertEqual(
            _sparse_dimension("Qdrant/bm42-all-minilm-l6-v2-attentions"),
            PGVECTOR_MAX_SPARSE_DIMENSIONS,
        )

    def test_hashed_sparse_indices_are_folded_and_collisions_are_merged(self):
        vector = SparseEmbedding(
            indices=np.array([4, 14, 9]),
            values=np.array([0.5, 0.25, 1.0]),
        )

        result = _normalise_sparse_vector(vector, dimension=10)

        self.assertEqual(result["indices"], [4, 9])
        self.assertEqual(result["values"], [0.75, 1.0])
        self.assertEqual(result["dimension"], 10)

    def test_store_does_not_retry_data_errors(self):
        from psycopg.errors import DataException

        state = self.FailedState(DataException("invalid sparse vector"))
        self.assertFalse(_retry_transient_store_error(None, None, state))

    def test_store_retries_infrastructure_errors(self):
        state = self.FailedState(ConnectionError("database unavailable"))
        self.assertTrue(_retry_transient_store_error(None, None, state))


class EmbeddingStorageTests(unittest.TestCase):
    def test_pgvector_literals(self):
        self.assertEqual(dense_vector_literal([1, 0.25]), "[1.0,0.25]")
        self.assertEqual(
            sparse_vector_literal(
                {
                    "indices": [4, 0],
                    "values": [0.5, 1],
                    "dimension": 10,
                }
            ),
            "{1:1.0,5:0.5}/10",
        )

    def test_sparse_counts_must_match(self):
        with self.assertRaises(ValueError):
            sparse_vector_literal({"indices": [0, 1], "values": [0.5], "dimension": 10})

    def test_sparse_indices_must_fit_dimension(self):
        with self.assertRaises(ValueError):
            sparse_vector_literal({"indices": [10], "values": [0.5], "dimension": 10})

    @patch("embeddings.storage.connect")
    def test_hybrid_search_uses_both_query_vectors(self, connect):
        cursor = connect.return_value.__enter__.return_value.cursor.return_value
        cursor.__enter__.return_value = cursor
        cursor.fetchall.return_value = [{"content": "result"}]

        results = hybrid_search(
            [0.25, 0.5],
            {"indices": [2], "values": [0.75], "dimension": 10},
            "dense-model",
            "sparse-model",
            4,
        )

        self.assertEqual(results, [{"content": "result"}])
        sql, parameters = cursor.execute.call_args.args
        self.assertIn("FULL OUTER JOIN sparse", sql)
        self.assertIn("[0.25,0.5]", parameters)
        self.assertIn("{3:0.75}/10", parameters)
        self.assertEqual(parameters[-1], 4)


if __name__ == "__main__":
    unittest.main()
