from __future__ import annotations

__version__ = "1.0.0"

from typing import Any


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "astro-provider-anyscale",  # Required
        "name": "Anyscale",  # Required
        "description": "A anyscale template for Apache Airflow providers.",  # Required
        "connection-types": [
            {"connection-type": "anyscale", "hook-class-name": "anyscale_provider.hooks.anyscale.AnyscaleHook"}
        ],
        "versions": [__version__],  # Required
    }
