import unittest

from pydantic import ValidationError

from ingestion.config import load_config
from releases import Release, parse_release


class ReleaseTests(unittest.TestCase):
    def test_parses_operator_references(self):
        release = Release(
            name="release-2",
            extractor="tika@1",
            chunker="chunker@2",
            embedders=["hybrid@1", "hybrid@2"],
        )

        self.assertEqual(str(release.extractor_ref), "tika@1")
        self.assertEqual(release.chunker_ref.name, "chunker")
        self.assertEqual(
            [str(ref) for ref in release.embedder_refs], ["hybrid@1", "hybrid@2"]
        )
        self.assertEqual(release.recogniser_refs, [])

    def test_a_release_needs_a_name(self):
        with self.assertRaises(ValidationError):
            Release(name="", extractor="tika@1", chunker="chunker@2")

    def test_rejects_a_config_with_no_active_release(self):
        with self.assertRaises(ValueError):
            parse_release({"concurrency_limits": {"tika": 4}})

    def test_the_shipped_config_names_an_active_release(self):
        release = parse_release(load_config())

        self.assertEqual(release.extractor, "tika@1")
        self.assertEqual(release.chunker, "chunker@2")
        self.assertIn("hybrid@1", release.embedders)


if __name__ == "__main__":
    unittest.main()
