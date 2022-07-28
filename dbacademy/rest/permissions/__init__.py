from dbacademy.rest.common import ApiContainer, ApiClient

__all__ = ["Permissions"]


class Permissions(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client

        from dbacademy.rest.permissions.jobs import Jobs
        self.jobs = Jobs(client)

        from dbacademy.rest.permissions.pools import Pools
        self.pools = Pools(client)

        from dbacademy.rest.permissions.sql import Sql
        self.sql = Sql(client)

        from dbacademy.rest.permissions.cluster_policies import ClusterPolicies
        self.cluster_policies = ClusterPolicies(client)
