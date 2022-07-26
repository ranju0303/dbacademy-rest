from __future__ import annotations

from dbacademy.dbrest import DBAcademyRestClient


class UcClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def __call__(self) -> UcClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def metastore_summary(self):
        return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/unity-catalog/metastore_summary")

    def set_default_metastore(self, workspace_id, catalog_name, metastore_id) -> str:
        payload = {
            "default_catalog_name": catalog_name,
            "metastore_id": metastore_id
        }
        return self.client.execute_put_json(f"{self.client.endpoint}/api/2.0/unity-catalog/workspaces/{workspace_id}/metastore", payload)
