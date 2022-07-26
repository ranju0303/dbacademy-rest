from __future__ import annotations

from dbacademy.dbrest import DBAcademyRestClient


class TokensClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/token"

    def __call__(self) -> TokensClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def list(self):
        results = self.client.execute_get_json(f"{self.base_url}/list")
        return results["token_infos"]

    def create(self, comment: str, lifetime_seconds: int):
        params = {
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.client.execute_post_json(f"{self.base_url}/create", params)

    def revoke(self, token_id):
        params = {
            "token_id": token_id
        }
        return self.client.execute_post_json(f"{self.base_url}/delete", params)
