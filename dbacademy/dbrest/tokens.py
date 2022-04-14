from dbacademy.dbrest import DBAcademyRestClient


class TokensClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_url = f"{self.endpoint}/api/2.0/token/list"

    def list(self):
        results = self.client.execute_get_json(f"{self.base_url}")
        return results["token_infos"]
