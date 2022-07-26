from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class JobsPermissionsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_uri = f"{self.client.endpoint}/api/2.0/permissions/jobs"

    def get_levels(self, id):
        return self.client.execute_get_json(f"{self.base_uri}/{id}/permissionLevels")

    def get(self, id):
        return self.client.execute_get_json(f"{self.base_uri}/{id}")
