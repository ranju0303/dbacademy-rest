from dbacademy.dbrest import DBAcademyRestClient


class TokenManagementClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_url = f"{self.endpoint}/api/2.0/token-management"

    def create_on_behalf_of_service_principal(self, application_id: str, comment: str, lifetime_seconds: int):
        params = {
            "application_id": application_id,
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.client.execute_post_json(f"{self.base_url}/on-behalf-of/tokens", params)
