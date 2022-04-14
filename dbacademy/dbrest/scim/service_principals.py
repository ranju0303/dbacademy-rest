from dbacademy.dbrest import DBAcademyRestClient


class ScimServicePrincipalsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint
        self.base_url = f"{self.endpoint}/api/2.0/preview/scim/v2/ServicePrincipals"

    def list(self):
        response = self.client.execute_get(f"{self.base_url}")
        return response
        # users = response.get("Resources", list())
        # totalResults = response.get("totalResults")
        # assert len(users) == int(totalResults), f"The totalResults ({totalResults}) does not match the number of records ({len(users)}) returned"
        # return users
