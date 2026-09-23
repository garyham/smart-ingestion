import unittest

from contracts.chunks import ChunkDraft
from operators import chunkers
from operators.chunkers import UnknownChunker
from operators.chunkers.chunker_v2 import ChunkerV2


class RegistryTests(unittest.TestCase):
    def test_finds_a_chunker_by_version(self):
        chunker = chunkers.get("chunker@2")
        self.assertIsInstance(chunker, ChunkerV2)
        self.assertEqual(str(chunker.ref), "chunker@2")

    def test_rejects_an_unknown_chunker(self):
        with self.assertRaises(UnknownChunker):
            chunkers.get("chunker@1")


class ChunkerV2Tests(unittest.TestCase):
    def setUp(self):
        self.chunker = ChunkerV2()

    def test_splits_by_heading_then_by_size(self):
        text = "# One\n\n" + ("alpha " * 400) + "\n\n## Two\n\nbeta\n"

        chunks = self.chunker.split(text)

        self.assertGreater(len(chunks), 2)
        self.assertTrue(all(isinstance(chunk, ChunkDraft) for chunk in chunks))
        self.assertEqual([chunk.index for chunk in chunks], list(range(len(chunks))))
        self.assertIn("One", chunks[0].headings)
        self.assertIn("Two", chunks[-1].headings)

    def test_splitting_needs_no_store_and_no_network(self):
        chunks = self.chunker.split("# One\n\nhello")
        self.assertEqual(chunks[0].text, "hello")

    def test_blank_text_is_no_chunks(self):
        self.assertEqual(self.chunker.split("  \n"), [])

    def test_a_chunker_takes_text_and_nothing_else(self):
        self.assertFalse(hasattr(self.chunker, "extract"))


if __name__ == "__main__":
    unittest.main()
