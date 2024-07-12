import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from haystack import Document, component, logging
from haystack.components.converters.utils import get_bytestream_from_source, normalize_metadata
from haystack.dataclasses import ByteStream
from haystack.lazy_imports import LazyImport

logger = logging.getLogger(__name__)

with LazyImport("Run 'pip install jq'") as jq_import:
    import jq


@component
class JSONToDocument:
    """
    Converts JSON files to Documents.

    Usage example:
    ```python
    from haystack.components.converters.json import JSONToDocument

    converter = JSONToDocument(jq_schema=".data[]"))
    results = converter.run(sources=["sample.json"], meta={"date_added": datetime.now().isoformat()})
    documents = results["documents"]
    print(documents[0].content)
    # 'This is the content from the JSON item.'
    ```

    """

    def __init__(
        self,
        jq_schema: str,
        content_key: Optional[str] = None,
        is_content_key_jq_parsable: bool = False,
        is_json_lines: bool = False,
    ):
        """
        Create an JSONToDocument component.

        :param jq_schema:
            The jq schema to use to extract the data or text from the JSON.
        :param content_key:
            The key to use to extract the content from the JSON.
            If is_content_key_jq_parsable is True, this has to be a jq-compatible schema.
            If is_content_key_jq_parsable is False, this should be a simple string key.
        :param is_content_key_jq_parsable:
            A flag to determine if content_key is parsable by jq or not.
            If True, content_key is treated as a jq schema and compiled accordingly.
            If False or if content_key is None, content_key is used as a simple string. Default is False.
        :param is_json_lines:
            Boolean flag to indicate whether the input is in JSON Lines format.

        """
        jq_import.check()

        self._jq_schema = jq.compile(jq_schema)
        self._content_key = content_key
        self._is_content_key_jq_parsable = is_content_key_jq_parsable
        self._is_json_lines = is_json_lines

    @component.output_types(documents=List[Document])
    def run(
        self,
        sources: List[Union[str, Path, ByteStream]],
        meta: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    ) -> Dict[str, List[Document]]:
        """
        Converts JSON files to Documents.

        :param sources:
            List of file paths or ByteStream objects.
        :param meta:
            Optional metadata to attach to the Documents.
            This value can be either a list of dictionaries or a single dictionary.
            If it's a single dictionary, its content is added to the metadata of all produced Documents.
            If it's a list, the length of the list must match the number of sources, because the two lists will
            be zipped.
            If `sources` contains ByteStream objects, their `meta` will be added to the output Documents.

        :returns:
            A dictionary with the following keys:
            - `documents`: Created Documents
        """
        documents = []
        meta_list = normalize_metadata(meta, sources_count=len(sources))

        for source, metadata in zip(sources, meta_list):
            try:
                bytestream: ByteStream = get_bytestream_from_source(source)
            except Exception as byte_e:
                logger.warning("Could not read '{source}'. Skipping it. Error: {error}", source=source, error=byte_e)
                continue
            try:
                if self._is_json_lines:
                    json_data = [json.loads(line) for line in bytestream.data.splitlines()]
                else:
                    json_data = json.loads(bytestream.data)

            except Exception as parse_e:
                logger.warning(
                    "Could not deserialize '{source}' as {file_format}. Skipping it. Error: {error}",
                    source=source,
                    file_format="JSON Lines" if self._is_json_lines else "JSON",
                    error=parse_e,
                )
                continue
            try:
                jq_result = self._jq_schema.input(json_data)
                for index, item in enumerate(jq_result):
                    content = self._get_content(item)
                    merged_meta = {**bytestream.meta, **metadata, "source": source, "seq_num": index}
                    new_document = Document(content=content, meta=merged_meta)
                    documents.append(new_document)

            except Exception as convert_e:
                logger.warning(
                    "Could not convert '{source}' to Document. Skipping it. Error: {error}",
                    source=source,
                    error=convert_e,
                )
                continue

        return {"documents": documents}

    def _get_content(self, item: Dict) -> str:
        """Extract and format content from a JSON item."""
        if self._content_key is not None:
            if self._is_content_key_jq_parsable:
                compiled_content_key = jq.compile(self._content_key)
                content = compiled_content_key.input(item).first()
            else:
                content = item[self._content_key]
        else:
            content = item

        if isinstance(content, str):
            return content
        elif isinstance(content, dict):
            return json.dumps(content) if content else ""
        else:
            return str(content) if content is not None else ""
