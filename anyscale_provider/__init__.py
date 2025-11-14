from __future__ import annotations

__version__ = "1.1.0"

from typing import Any

import airflow
from packaging import version as pkg_version

_IS_AIRFLOW_3 = pkg_version.parse(airflow.__version__).major == 3


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
