"""
Modules shared between dbrest and dougrest.  Forms the basis of a shared unified rest client.
"""

from dbacademy.dougrest.client import DatabricksApi, DatabricksApiException

__all__ = ["DatabricksApi", "DatabricksApiException", "databricks"]

databricks = DatabricksApi.default_client
