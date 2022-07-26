from __future__ import annotations
from dbacademy.dbrest import DBAcademyRestClient

class SqlClient():
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.dbrest.sql.config import SqlConfigClient
        self.config = SqlConfigClient(self.client)

        from dbacademy.dbrest.sql.endpoints import SqlEndpointsClient
        self.endpoints = SqlEndpointsClient(self.client)

        from dbacademy.dbrest.sql.queries import SqlQueriesClient
        self.queries = SqlQueriesClient(self.client)

        self.permissions = client.permissions.sql

    def __call__(self) -> SqlClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self
