from __future__ import annotations
from dbacademy.dbrest import DBAcademyRestClient


class ClusterPolicyClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/policies/clusters"

    def __call__(self) -> ClusterPolicyClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def get_by_id(self):
        pass

    def get_by_name(self):
        pass

    def list(self):
        return self.client.execute_get_json(f"{self.base_uri}/list")

    def create(self):
        pass

    def update(self):
        pass  # create

    def delete(self):
        pass

    def create_or_update(self):
        pass

    # def list(self):
    #     return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/clusters/list")
    #
    # def get(self, cluster_id):
    #     return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")
    #
    # def get_current_spark_version(self):
    #     from dbacademy import dbgems
    #     cluster_id = dbgems.get_tags()["clusterId"]
    #     return self.get(cluster_id).get("spark_version", None)
    #
    # def get_current_instance_pool_id(self):
    #     from dbacademy import dbgems
    #     cluster_id = dbgems.get_tags()["clusterId"]
    #     return self.get(cluster_id).get("instance_pool_id", None)
    #
    # def get_current_node_type_id(self):
    #     from dbacademy import dbgems
    #     cluster_id = dbgems.get_tags()["clusterId"]
    #     return self.get(cluster_id).get("node_type_id", None)
