from dbacademy.dbrest import DBAcademyRestClient


class TokensClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_url = f"{self.endpoint}/api/2.0/token"

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
