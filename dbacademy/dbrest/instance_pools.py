from __future__ import annotations
from dbacademy.dbrest import DBAcademyRestClient


class InstancePoolsClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/instance-pools"

    def __call__(self) -> InstancePoolsClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def get_by_id(self, instance_pool_id):
        return self.client.execute_get_json(f"{self.base_uri}/get?instance_pool_id={instance_pool_id}")

    def get_by_name(self, name):
        pools = self.list()
        for pool in pools:
            if pool.get("instance_pool_name") == name:
                return pool
        return None

    def list(self):
        # Does not support pagination
        return self.client.execute_get_json(f"{self.base_uri}/list").get("instance_pools", [])

    def create(self, name: str, definition: dict):
        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        definition["instance_pool_name"] = name

        return self.client.execute_post_json(f"{self.base_uri}/create", params=definition)

    def update_by_name(self, name: str, definition: dict):
        pool = self.get_by_name(name)
        assert pool is not None, f"A pool named \"{name}\" was not found."

        instance_pool_id = pool.get("instance_pool_id")

        return self.update_by_id(instance_pool_id, name, definition)

    def update_by_id(self, instance_pool_id: str, name: str, definition: dict):
        import json
        assert type(instance_pool_id) == str, f"Expected id to be of type str, found {type(instance_pool_id)}"
        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        params = {
            "instance_pool_id": instance_pool_id,
            "instance_pool_name": name,
            "definition": json.dumps(definition)
        }
        return self.client.execute_post_json(f"{self.base_uri}/edit", params=params)

    def create_or_update(self, name, definition):
        pool = self.get_by_name(name)

        if pool is None:
            self.create(name, definition)
        else:
            instance_pool_id = pool.get("instance_pool_id")
            self.update_by_id(instance_pool_id, name, definition)

    def delete_by_id(self, instance_pool_id):
        return self.client.execute_post_json(f"{self.base_uri}/delete", params={"instance_pool_id": instance_pool_id})

    def delete_by_name(self, name):
        pool = self.get_by_name(name)
        assert pool is not None, f"A pool named \"{name}\" was not found."

        instance_pool_id = pool.get("instance_pool_id")
        return self.delete_by_id(instance_pool_id)
