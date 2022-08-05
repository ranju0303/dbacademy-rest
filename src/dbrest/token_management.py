from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class TokenManagementClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/token-management"

    def create_on_behalf_of_service_principal(self, application_id: str, comment: str, lifetime_seconds: int):
        params = {
            "application_id": application_id,
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.client.execute_post_json(f"{self.base_url}/on-behalf-of/tokens", params)
