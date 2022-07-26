from __future__ import annotations

from dbacademy.dbrest import DBAcademyRestClient


class TokenManagementClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/token-management"

    def __call__(self) -> TokenManagementClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def create_on_behalf_of_service_principal(self, application_id: str, comment: str, lifetime_seconds: int):
        params = {
            "application_id": application_id,
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.client.execute_post_json(f"{self.base_url}/on-behalf-of/tokens", params)
