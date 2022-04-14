from dbacademy.dbrest import DBAcademyRestClient


class ScimServicePrincipalsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint
        self.base_url = f"{self.endpoint}/api/2.0/preview/scim/v2/ServicePrincipals"

    def list(self):
        response = self.client.execute_get_json(f"{self.base_url}")
        return response
        # sps = response.get("Resources", list())
        # totalResults = response.get("totalResults")
        # assert len(users) == int(totalResults), f"The totalResults ({totalResults}) does not match the number of records ({len(users)}) returned"
        # return users

    def get_by_id(self, service_principle_id: str):
        return self.client.execute_get_json(f"{self.base_url}/{service_principle_id}")

    def create(self, display_name: str, group_ids: list = [], entitlements: list = []):
        params = {
            "displayName": display_name,
            "entitlements": [],
            "groups": [],
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "active": True
        }

        group_ids = group_ids if group_ids is not None else list()
        for group_id in group_ids:
            value = {"value": group_id}
            params["groups"].append(value)

        entitlements = entitlements if entitlements is not None else list()
        for entitlement in entitlements:
            value = {"value": entitlement}
            params["entitlements"].append(value)

        return self.client.execute_post_json(f"{self.base_url}", params)
