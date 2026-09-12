#!/usr/bin/env python3
import os, unittest
from unittest.mock import patch
import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('check_docs', Path(__file__).with_name('check-docs.py'))
checker=importlib.util.module_from_spec(spec); spec.loader.exec_module(checker)

class DocsRuntimeConfigTest(unittest.TestCase):
    def test_postgres_url_validation_rejects_unsafe_values_before_process_start(self):
        invalid=['mysql://localhost:3306/db','postgres://host/db#frag','postgres://host','postgres://host/db%zz','postgres://host:99999/db']
        for value in invalid:
            with self.subTest(value=value), self.assertRaisesRegex(AssertionError,'PostgreSQL'):
                checker.parse_postgres_test_url(value)

    def test_postgres_url_validation_accepts_supported_url(self):
        parsed=checker.parse_postgres_test_url('postgres://relayhub:secret@127.0.0.1:5432/relayhub_test?sslmode=disable')
        self.assertEqual(parsed.hostname,'127.0.0.1')
        self.assertEqual(parsed.path,'/relayhub_test')

    def test_runtime_requires_explicit_postgres_url(self):
        with patch.dict(os.environ, {'RELAYHUB_DOCS_TEST_POSTGRES_URL':''}, clear=False):
            with self.assertRaisesRegex(AssertionError,'RELAYHUB_DOCS_TEST_POSTGRES_URL'):
                with checker.runtime():
                    pass

if __name__ == '__main__':
    unittest.main()
