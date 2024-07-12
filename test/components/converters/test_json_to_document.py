# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from haystack.components.converters.json import JSONToDocument
from unittest.mock import patch
from haystack.dataclasses import ByteStream


class TestJSONToDocument:
    def test_init(self):
        assert isinstance(JSONToDocument(jq_schema=".spam[]"), JSONToDocument)

    def test_default_initialization_parameters(self):
        converter = JSONToDocument(jq_schema=".spam[]")
        assert converter._content_key is None
        assert converter._is_content_key_jq_parsable is False
        assert converter._additional_meta is None

    def test_custom_initialization_parameters(self):
        converter = JSONToDocument(
            jq_schema=".spam[]",
            content_key="eggs",
            is_content_key_jq_parsable=True,
            additional_meta_fields=["spam", "eggs", "ham"],
        )

        assert converter._content_key == "eggs"
        assert converter._is_content_key_jq_parsable is True
        assert converter._additional_meta == ["spam", "eggs", "ham"]

    def test_run_without_content_key(self, test_files_path):
        converter = JSONToDocument(jq_schema=".prizes[].category")
        file_path = test_files_path / "json" / "prize.json"
        sources = [file_path]
        results = converter.run(sources=sources)
        documents = results["documents"]

        expected_content = ["physics", "chemistry", "peace"]

        # assert one document created for each prize.category
        assert len(documents) == 3

        # assert content extracted correctly
        for doc, content in zip(documents, expected_content):
            assert doc.content == content

    def test_run_with_content_key(self, test_files_path):
        converter = JSONToDocument(jq_schema=".prizes[].laureates[]?", content_key="motivation")
        file_path = test_files_path / "json" / "prize.json"
        sources = [file_path]
        results = converter.run(sources=sources)
        documents = results["documents"]

        expected_content = [
            "for groundbreaking experiments regarding entangled quantum states",
            "for groundbreaking research in early childhood education",
            "for the development of chemical tools used in biochemistry",
            "for efforts to build peace and combat violence",
        ]

        # assert one document created for each prize.laureates.motivation
        assert len(documents) == 4

        # assert content extracted correctly
        for doc, content in zip(documents, expected_content):
            assert doc.content == content

    def test_run_without_additional_meta_fields(self, test_files_path):
        converter = JSONToDocument(jq_schema=".prizes[].laureates[]?", content_key="motivation")
        file_path = test_files_path / "json" / "prize.json"
        sources = [file_path]
        results = converter.run(sources=sources)
        documents = results["documents"]
        expected_meta = [
            {"file_path": str(file_path)},
            {"file_path": str(file_path)},
            {"file_path": str(file_path)},
            {"file_path": str(file_path)},
        ]

        # assert metadata extracted correctly
        for doc, meta in zip(documents, expected_meta):
            assert doc.meta == meta

    def test_run_with_additional_meta_fields(self, test_files_path):
        converter = JSONToDocument(
            jq_schema=".prizes[].laureates[]?",
            content_key="motivation",
            additional_meta_fields=["id", "firstname", "surname", "dateOfBirth"],
        )
        file_path = test_files_path / "json" / "prize.json"
        sources = [file_path]
        results = converter.run(sources=sources)
        documents = results["documents"]
        expected_meta = [
            {
                "file_path": str(file_path),
                "id": 1,
                "firstname": "John",
                "surname": "Williams",
                "dateOfBirth": "1975-03-15",
            },
            {
                "file_path": str(file_path),
                "id": 2,
                "firstname": "Susan",
                "surname": "Johnson",
                "dateOfBirth": "1982-09-22",
            },
            {
                "file_path": str(file_path),
                "id": 3,
                "firstname": "Jane",
                "surname": "Smith",
                "dateOfBirth": "1978-11-30",
            },
            {
                "file_path": str(file_path),
                "id": 4,
                "firstname": "Maria",
                "surname": "Garcia",
                "dateOfBirth": "1970-06-08",
            },
        ]

        # assert metadata extracted correctly
        for doc, meta in zip(documents, expected_meta):
            assert doc.meta == meta

    def test_run_with_meta_parameter(self, test_files_path):
        converter = JSONToDocument(jq_schema=".prizes[].laureates[]?", content_key="motivation")
        file_path = test_files_path / "json" / "prize.json"
        sources = [file_path]
        meta = {"spam": "eggs"}
        results = converter.run(sources=sources, meta=meta)
        documents = results["documents"]
        expected_meta = [{"file_path": str(file_path), **meta}] * 4

        # assert metadata added correctly
        for doc, meta in zip(documents, expected_meta):
            assert doc.meta == meta
