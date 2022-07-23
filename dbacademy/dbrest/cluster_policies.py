from __future__ import annotations
from dbacademy.dbrest import DBAcademyRestClient


class ClusterPolicyClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/policies/clusters"

    def __call__(self) -> ClusterPolicyClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def get_by_id(self, policy_id):
        return self.client.execute_get_json(f"{self.base_uri}/get?policy_id={policy_id}")

    def get_by_name(self, name):
        policies = self.list()
        for policy in policies:
            if policy.get("name") == name:
                return policy
        return None

    def list(self):
        # Does not support pagination
        return self.client.execute_get_json(f"{self.base_uri}/list").get("policies", [])

    def create(self, name: str, definition: dict):
        import json
        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        params = {
            "name": name,
            "definition": json.dumps(definition)
        }
        return self.client.execute_post_json(f"{self.base_uri}/create", params=params)

    def update(self):
        pass  # create

    def delete_by_id(self):
        pass

    def delete_by_name(self, name):
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
