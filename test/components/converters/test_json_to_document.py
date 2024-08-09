# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
import logging
from pathlib import Path
import json
import pytest
import os


from haystack.components.converters.jq_json import JQToJSON


class TestJQJSONToDocument:
    
  


    def test_run(self, test_files_path,caplog):
        """
        Test if the component runs correctly, both standard JSON and JSON lines
        Using a subset of the nobel prize dataset - https://api.nobelprize.org/v1/prize.json
        Values to verify document against expected:
        1) Number of documents generated
        2) output[0].content =  .first() value of the jq query - Validate content field
        3) output[0].meta = output of metadata function + additional metadata - Validate metadata
        
        """
        # **** - Need to do JSON lines
        # Test data 
        file_path = test_files_path / "json" / "sample.json"
        file_path_lines = test_files_path / "json" / "sample_json_lines.json" #json lines
        # file_path.parent.mkdir(parents=True, exist_ok=True)
        jq_data_schema = '.prizes[].laureates[]?'
        jq_data_schema_lines = 'del(.share)'
        expected_content = {
        "id": "1029", "firstname": "Moungi", "surname": "Bawendi", 
        "motivation": "\"for the discovery and synthesis of quantum dots\"", 
        "share": "3"} 
        expected_meta = {
        'dataset': 'nobel data set', 'seq_no': 1, 'file_path': 'sample.json', 
        'id': '1029', 'firstname': 'Moungi', 'surname': 'Bawendi', 'share': '3','jq_schema': '.prizes[].laureates[]?'}
        expected_content_lines = {"year": "2023", "category": "chemistry", "id": "1029", "firstname": "Moungi", "surname": "Bawendi", "motivation": "\"for the discovery and synthesis of quantum dots\""}
        expected_meta_lines =  {'dataset': 'nobel data set', 'id': '1029', 'firstname': 'Moungi', 'surname': 'Bawendi', 
            'share': None, 'seq_no': 1, 
            'file_path': 'sample_json_lines.json', 'jq_schema': 'del(.share)'}




    

        # Metadata function
        def metadata_func(sample: dict) -> dict:
            metadata = {}
            metadata["id"] = sample.get("id")
            metadata["firstname"] = sample.get("firstname")
            metadata["surname"] = sample.get("surname")
            metadata["share"] = sample.get("share")
            return metadata

        converter = JQToJSON()
        with caplog.at_level(logging.WARNING):
            output_dict = converter.run(sources = file_path,jq_data_schema=jq_data_schema, metadata_func=metadata_func,
                                   metadata = {'dataset': 'nobel data set'},json_lines=False)
        
        #print ('file path  ', file_path, 'output:   ',output)
        #print("Current working directory:", os.getcwd())
        #print("Environment variables:", os.environ)
        output = output_dict["documents"]
        doc_dict = json.loads(output[0].content)
        meta = output[0].meta
        assert len(output) == 25
        assert doc_dict == expected_content
        assert meta == expected_meta
        
        # Test json_lines format
        
        converter_lines = JQToJSON()
        
        with caplog.at_level(logging.WARNING):
            output_dict = converter_lines.run(sources = file_path_lines,jq_data_schema=jq_data_schema_lines, metadata_func=metadata_func,
                                   metadata = {'dataset': 'nobel data set'},json_lines=True)
        
        #Look at first element
        output = output_dict ['documents']
        doc_dict = json.loads(output[0][0].content)
        meta = output[0][0].meta
        
        assert len(output) == 23
        assert doc_dict == expected_content_lines
        assert meta == expected_meta_lines

        
        return

    def test_run_error_handling(self, test_files_path, caplog):
        """
        Test if the component correctly handles errors.
        """
        #set up
        file_path_error = "non_existing_file.json"
        converter_error = JQToJSON()

        with caplog.at_level(logging.WARNING):
            output = converter_error.run(sources = file_path_error,jq_data_schema='', metadata_func=None,
                                   metadata = None,json_lines=False)
        assert "non_existing_file.json" in caplog.text
        return

