import logging
import os

import requests
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

_SCOPE = "https://management.azure.com/.default"


def purge_cache(content_paths: list[str] | None = None) -> bool:
    """Purge Azure Front Door cache via the ARM REST API.

    Requires env vars:
      AZURE_FRONTDOOR_SUBSCRIPTION_ID
      AZURE_FRONTDOOR_RESOURCE_GROUP
      AZURE_FRONTDOOR_NAME

    Returns True on success, False on failure or missing config.
    """
    subscription_id = os.getenv("AZURE_FRONTDOOR_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_FRONTDOOR_RESOURCE_GROUP")
    frontdoor_name = os.getenv("AZURE_FRONTDOOR_NAME")

    if not all([subscription_id, resource_group, frontdoor_name]):
        logger.warning("Front Door purge skipped: missing AZURE_FRONTDOOR_* env vars")
        return False

    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Network/frontDoors/{frontdoor_name}"
        f"/purge?api-version=2021-06-01"
    )

    paths = content_paths or ["/*"]

    try:
        credential = DefaultAzureCredential()
        token = credential.get_token(_SCOPE).token

        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"contentPaths": paths},
            timeout=30,
        )
        resp.raise_for_status()
        logger.info("Front Door cache purge accepted (status %s) for paths: %s", resp.status_code, paths)
        return True
    except Exception as e:
        logger.error("Front Door cache purge failed: %s", e)
        return False
