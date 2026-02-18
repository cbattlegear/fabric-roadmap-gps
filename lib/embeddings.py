import os
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

_client = None
_available = None


def _get_client():
    global _client, _available
    if _available is not None:
        return _client
    try:
        from openai import AzureOpenAI
        endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        if not endpoint or not api_key:
            _available = False
            return None
        _client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-02-01"
        )
        _available = True
        return _client
    except Exception as e:
        logger.warning("Azure OpenAI not available: %s", e)
        _available = False
        return None


def get_embedding(text: str) -> Optional[List[float]]:
    """Generate an embedding for the given text using Azure OpenAI."""
    client = _get_client()
    if client is None:
        return None
    deployment = os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT', 'text-embedding-3-small')
    try:
        response = client.embeddings.create(input=text, model=deployment)
        return response.data[0].embedding
    except Exception as e:
        logger.error("Embedding generation failed: %s", e)
        return None


def is_available() -> bool:
    """Check if embedding generation is configured and available."""
    _get_client()
    return bool(_available)
