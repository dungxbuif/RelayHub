#!/usr/bin/env python3
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CHECKER_PATH = ROOT / "backend/scripts/check-docs.py"


def load_checker():
    spec = importlib.util.spec_from_file_location("check_docs", CHECKER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ProductCatalogTests(unittest.TestCase):
    def test_product_catalog_cli_accepts_public_catalog(self):
        result = subprocess.run(
            [sys.executable, str(CHECKER_PATH), "--product-catalog"],
            cwd=ROOT / "backend",
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        self.assertIn("PASS: product catalog", result.stdout)

    def test_planned_capability_cannot_be_published_as_released(self):
        checker = load_checker()
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest_copy = temp / "content-manifest.json"
            capabilities_copy = temp / "capabilities.json"
            manifest_copy.write_text(json.dumps({
                "schema_version": 1,
                "primary_version": "v1",
                "versions": [{
                    "id": "v1",
                    "status": "preview",
                    "pages": [{
                        "id": "intro",
                        "source": "docs/intro.md",
                        "public_url": "/",
                        "text_path": "intro.md",
                        "capability_ids": ["collaborative-rooms"],
                    }],
                }],
                "aliases": [],
            }))
            capabilities = {"capabilities": [{
                "id": "collaborative-rooms",
                "product_version": "v1",
                "status": "released",
                "source_commit": "",
                "evidence_paths": [],
                "api_contract": "",
                "sdk_versions": [],
                "limitations": [],
            }]}
            capabilities_copy.write_text(json.dumps(capabilities))
            with self.assertRaises(AssertionError):
                checker.check_product_catalog(
                    manifest_path=manifest_copy,
                    capabilities_path=capabilities_copy,
                    docs_root=ROOT / "web/docs/docs",
                )


if __name__ == "__main__":
    unittest.main()
