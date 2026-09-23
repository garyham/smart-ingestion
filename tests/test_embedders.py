import unittest
from unittest.mock import Mock, patch

import numpy as np
from fastembed import SparseEmbedding

from contracts.embeddings import PGVECTOR_MAX_SPARSE_DIMENSIONS, Embedding, SparseVector
from operators import embedders
from operators.embedders import UnknownEmbedder, hybrid_v1
from operators.embedders.hybrid_v1 import HybridV1
from store.vectors import dense_vector_literal, sparse_vector_literal


class VectorTests(unittest.TestCase):
    def test_dense_literal(self):
        self.assertEqual(dense_vector_literal([0.5, -1.0]), "[0.5,-1.0]")

    def test_sparse_literal_is_one_based(self):
        vector = SparseVector(indices=[2, 0], values=[0.5, 0.25], dimension=4)
        self.assertEqual(sparse_vector_literal(vector), "{1:0.25,3:0.5}/4")

    def test_a_sparse_vector_rejects_an_out_of_range_index(self):
        with self.assertRaises(ValueError):
            SparseVector(indices=[9], values=[1.0], dimension=4)

    def test_a_sparse_vector_rejects_mismatched_lengths(self):
        with self.assertRaises(ValueError):
            SparseVector(indices=[0, 1], values=[1.0], dimension=4)

    def test_a_sparse_vector_fits_pgvector(self):
        with self.assertRaises(ValueError):
            SparseVector(
                indices=[], values=[], dimension=PGVECTOR_MAX_SPARSE_DIMENSIONS + 1
            )


class RegistryTests(unittest.TestCase):
    def test_finds_an_embedder_by_version(self):
        embedder = embedders.get("hybrid@1", device="cpu")
        self.assertIsInstance(embedder, HybridV1)
        self.assertEqual(embedder.device, "cpu")

    def test_rejects_an_unknown_embedder(self):
        with self.assertRaises(UnknownEmbedder):
            embedders.get("hybrid@99")


class HybridV1Tests(unittest.TestCase):
    def test_config_leaves_the_execution_device_out(self):
        self.assertNotIn("device", HybridV1.CONFIG)
        self.assertIn("dense_model", HybridV1.CONFIG)
        self.assertIn("sparse_model", HybridV1.CONFIG)

    def test_normalises_sparse_indices_into_the_dimension(self):
        vector = SparseEmbedding(
            indices=np.array([1, 5]), values=np.array([0.5, 0.25])
        )
        normalised = hybrid_v1.normalise_sparse_vector(vector, 4)
        self.assertTrue(all(index < 4 for index in normalised.indices))

    def test_merges_indices_that_collide_after_folding(self):
        vector = SparseEmbedding(
            indices=np.array([1, 5]), values=np.array([0.5, 0.25])
        )
        normalised = hybrid_v1.normalise_sparse_vector(vector, 4)
        self.assertEqual(normalised.indices, [1])
        self.assertAlmostEqual(normalised.values[0], 0.75)

    def test_bm42_uses_the_pgvector_dimension_ceiling(self):
        self.assertEqual(
            hybrid_v1.sparse_dimension(HybridV1.CONFIG["sparse_model"]),
            PGVECTOR_MAX_SPARSE_DIMENSIONS,
        )

    def test_rejects_an_unknown_sparse_model(self):
        with self.assertRaises(ValueError):
            hybrid_v1.sparse_dimension("nobody/such-model")

    def test_rejects_an_unknown_device(self):
        with self.assertRaises(ValueError):
            hybrid_v1.available_embedding_providers("tpu")

    def test_search_options_name_the_version_and_its_ranking(self):
        options = HybridV1(device="cpu").search_options()

        self.assertEqual(str(options.embedder), "hybrid@1")
        self.assertEqual(options.limit, 3)
        self.assertEqual(options.fusion_k, 60)
        self.assertEqual(options.candidates, 50)
        self.assertEqual(HybridV1(device="cpu").search_options(20).candidates, 100)

    def test_embeds_each_text_both_ways_in_order(self):
        dense = Mock()
        dense.embed.return_value = [np.array([0.1, 0.2]), np.array([0.3, 0.4])]
        sparse = Mock()
        sparse.embed.return_value = [
            SparseEmbedding(indices=np.array([0]), values=np.array([1.0])),
            SparseEmbedding(indices=np.array([1]), values=np.array([2.0])),
        ]
        with patch.object(hybrid_v1, "_dense_model", return_value=dense), patch.object(
            hybrid_v1, "_sparse_model", return_value=sparse
        ):
            embedded = HybridV1(device="cpu").embed(["first", "second"])

        self.assertTrue(all(isinstance(item, Embedding) for item in embedded))
        self.assertEqual([item.dense for item in embedded], [[0.1, 0.2], [0.3, 0.4]])
        self.assertEqual([item.sparse.indices for item in embedded], [[0], [1]])


if __name__ == "__main__":
    unittest.main()
