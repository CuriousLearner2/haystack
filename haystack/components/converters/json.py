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

    def __init__(self, jq_schema: str, content_key: Optional[str] = None, metadata_keys: Optional[List[str]] = None):
        """
        Create an JSONToDocument component.

        :param jq_schema:
            The jq schema to use to extract the data or text from the JSON.
            This must be a jq-compatible schema.
            Example: `".data[]"`
        :param content_key:
            The key to use to extract the content from the JSON.
            This must be a jq-compatible schema.
            Example: `".text"`
        :param metadata_keys:
            A list of additional fields to extract from the JSON and add to the metadata.
            This must be a jq-compatible schema.
            Example: `[".author", ".date"]`
        """
        jq_import.check()

        self._jq = jq.compile(jq_schema)
        self._content_key = content_key
        self._metadata_keys = metadata_keys

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
                json_data = json.loads(bytestream.data)
            except Exception as parse_e:
                logger.warning(
                    "Could not deserialize '{source}'. Skipping it. Error: {error}", source=source, error=parse_e
                )
                continue
            try:
                jq_result = self._jq.input_value(json_data)

                for result in jq_result:
                    content = self._get_content(result)
                    meta = self._get_meta(result)
                    merged_meta = {**bytestream.meta, **metadata, **meta}
                    documents.append(Document(content=content, meta=merged_meta))

            except Exception as convert_e:
                logger.warning(
                    "Could not convert source `{source}` to Document. Skipping it.\nError: {error}",
                    source=source,
                    error=convert_e,
                )
                continue

        return {"documents": documents}

    def _get_content(self, json_record: Dict) -> str:
        """Extract and format content from a JSON item."""
        if self._content_key is not None:
            compiled_content_key = jq.compile(self._content_key)
            content = compiled_content_key.input_value(json_record).first()
        else:
            content = json_record

        if isinstance(content, str):
            return content
        elif isinstance(content, dict):
            return json.dumps(content) if content else ""
        else:
            return str(content) if content is not None else ""

    def _get_meta(self, json_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add additional metadata specified by metadata_keys argument.

        :param json_record: Single data payload.
        :return: A dictionary containing additional metadata fields.
        """
        if not self._metadata_keys:
            return {}

        meta = {}
        for field in self._metadata_keys:
            try:
                meta[field] = jq.compile(field).input_value(json_record).first()
            except Exception as e:
                # TODO: Fix this to log the error properly
                logger.warning("Failed to extract metadata field `{field}`\nError: {e}", field=field, e=e)

        return meta
