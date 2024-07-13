# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
import logging
from pathlib import Path
import json
import pytest

from haystack.dataclasses import ByteStream
from haystack.components.converters.json import JSONToDocument


class TestJSONToDocument:
    def test_run(self, test_files_path,caplog):
        """
        Test if the component runs correctly.
        """
        sample_json = {
            "store": {
                "book": [
                    {"category": "fiction", "price": 8.95, "title": "Book A"},
                    {"category": "non-fiction", "price": 12.99, "title": "Book B"}
                ]
            }
        }
        json_string = json.dumps (sample_json)

        # Save JSON content to a file
        file_path = test_files_path / "json" / "sample.json"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(sample_json, f)

        # Test all the sources
        source_list = ['string','file name','Path name','Bytestream']

        for source in source_list:
            if source == 'string':
                sources = json_string
                file_path = ''
            elif source == 'file name':
                sources = file_path
            elif source == 'Path name':
                sources = Path(file_path)
            else:
                bytestream = ByteStream.from_string(json_string, mime_type="application/json")
                sources = bytestream
                file_path = None
            converter = JSONToDocument()
            meta={"file_path": str(file_path)}
            with caplog.at_level(logging.WARNING):
                output = converter.run([sources],[meta])
            docs = output["documents"]
            assert len(docs) == 1
            assert "store_book_0_category" in docs[0].content
            assert docs[0].meta["file_path"] == str(file_path)
            file_path = test_files_path / "json" / "sample.json"

    def test_run_error_handling(self, test_files_path, caplog):
        """
        Test if the component correctly handles errors.
        """
        paths = [test_files_path / "json" / "sample.json", "non_existing_file.json"]
        converter = JSONToDocument()
        meta = [{"file_path": str(paths[0])},{'file_path': ''}]
        with caplog.at_level(logging.WARNING):
            output = converter.run(sources=paths, meta=meta)
            assert "non_existing_file.json" in caplog.text
        docs = output["documents"]
        assert len(docs) == 1
        assert docs[0].meta["file_path"] == str(paths[0])

    def test_run_with_meta(self, test_files_path):
        """
        Test if the component correctly merges metadata.
        """
        sample_json = {
            "store": {
                "book": [
                    {"category": "fiction", "price": 8.95, "title": "Book A"},
                    {"category": "non-fiction", "price": 12.99, "title": "Book B"}
                ]
            }
        }

        # Save JSON content to a file
        file_path = test_files_path / "json" / "sample.json"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(sample_json, f)

        converter = JSONToDocument()
        bytestream = ByteStream.from_file_path(file_path, meta={"author": "test_author"})
        output = converter.run(sources=[bytestream], meta={"language": "en"})
        document = output["documents"][0]

        # Validate the metadata merge
        assert document.meta["author"] == "test_author"
        assert document.meta["language"] == "en"
