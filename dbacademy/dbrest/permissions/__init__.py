from __future__ import annotations

from dbacademy.dbrest import DBAcademyRestClient


class SqlProxy():
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.dbrest.permissions.sql_endpoints import SqlEndpointsClient
        self.endpoints = SqlEndpointsClient(self.client)

        from dbacademy.dbrest.permissions.sql_permissions_client import SqlPermissionsClient
        self.queries = SqlPermissionsClient(self.client, "query", "queries")
        self.dashboards = SqlPermissionsClient(self.client, "dashboard", "dashboards")
        self.data_sources = SqlPermissionsClient(self.client, "data_source", "data_sources")
        self.alerts = SqlPermissionsClient(self.client, "alert", "alerts")

    def __call__(self) -> SqlProxy:
        """Returns itself.  Provided for backwards compatibility."""
        return self


class PermissionsClient():
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.sql = SqlProxy(self.client)

        from dbacademy.dbrest.permissions.jobs import JobsPermissionsClient
        self.jobs = JobsPermissionsClient(self.client)

    def __call__(self) -> PermissionsClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self
