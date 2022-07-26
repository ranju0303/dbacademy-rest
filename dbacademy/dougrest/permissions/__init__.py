from dbacademy.rest.common import ApiContainer, ApiClient


class Permissions(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        from dbacademy.dougrest.permissions.pools import Pools
        self.pools = Pools(client)
        from dbacademy.dougrest.permissions.sql_permissions import SqlPermissions
        self.queries = SqlPermissions(client, "query", "queries")
        self.dashboards = SqlPermissions(client, "dashboard")
        self.data_sources = SqlPermissions(client, "data_source")
        self.alerts = SqlPermissions(client, "alert")
