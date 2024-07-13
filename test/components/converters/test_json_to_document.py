# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from haystack.components.converters.json import JSONToDocument
import jq


class TestJSONToDocument:
    def test_init(self):
        assert isinstance(JSONToDocument(jq_schema=".spam[]"), JSONToDocument)

    def test_default_initialization_parameters(self):
        pass

    def test_custom_initialization_parameters(self):
        pass

    def test_run_without_content_key(self, test_files_path):
        pass

    def test_run_with_content_key(self, test_files_path):
        pass

    def test_run_without_metadata_keys(self, test_files_path):
        pass

    def test_run_with_metadata_keys(self, test_files_path):
        pass

    def test_run_with_meta_parameter(self, test_files_path):
        pass
