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
        from dbacademy import dbgems
        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        definition["instance_pool_name"] = name

        cloud = dbgems.get_cloud()
        if cloud == "AWS":
            node_type_id = definition.get("node_type_id", "i3.xlarge")

            # definition["aws_attributes"] = {
            #     "availability": "ON_DEMAND",
            #     "zone_id": "us-west-2d",
            #     "spot_bid_price_percent": 100
            # }
            # definition["enable_elastic_disk"] = False

        elif cloud == "MSA":
            pass
        elif cloud == "GCP":
            pass
        else:
            raise Exception(f"The cloud {cloud} is not supported.")

        # Initialize defaulted values
        definition["node_type_id"] = node_type_id

        pool = self.client.execute_post_json(f"{self.base_uri}/create", params=definition)
        return self.get_by_id(pool.get("instance_pool_id"))

    def update_by_name(self, instance_pool_name: str, min_idle_instances: int = None, max_capacity: int = None, idle_instance_autotermination_minutes: int = None):

        pool = self.get_by_name(instance_pool_name)
        assert pool is not None, f"A pool named \"{instance_pool_name}\" was not found."

        instance_pool_id = pool.get("instance_pool_id")

        return self.update_by_id(instance_pool_id=instance_pool_id,
                                 instance_pool_name=instance_pool_name,
                                 min_idle_instances=min_idle_instances,
                                 max_capacity=max_capacity,
                                 idle_instance_autotermination_minutes=idle_instance_autotermination_minutes)

    def update_by_id(self, instance_pool_id: str, instance_pool_name: str, min_idle_instances: int = None, max_capacity: int = None, idle_instance_autotermination_minutes: int = None):
        assert type(instance_pool_id) == str, f"Expected id to be of type str, found {type(instance_pool_id)}"
        assert instance_pool_name is None or type(instance_pool_name) == str, f"Expected name to be of type str, found {type(instance_pool_name)}"
        assert min_idle_instances is None or type(min_idle_instances) == int, f"Expected min_idle_instances to be of type int, found {type(min_idle_instances)}"
        assert max_capacity is None or type(max_capacity) == int, f"Expected max_capacity to be of type int, found {type(max_capacity)}"
        assert idle_instance_autotermination_minutes is None or type(idle_instance_autotermination_minutes) == int, f"Expected idle_instance_autotermination_minutes to be of type int, found {type(idle_instance_autotermination_minutes)}"

        pool = self.get_by_id(instance_pool_id)

        params = {
            "instance_pool_id": instance_pool_id,
            "instance_pool_name": pool.get("instance_pool_name") if instance_pool_name is None else instance_pool_name
        }

        if max_capacity is not None: params["max_capacity"] = max_capacity
        if min_idle_instances is not None: params["min_idle_instances"] = min_idle_instances
        if idle_instance_autotermination_minutes is not None: params["idle_instance_autotermination_minutes"] = idle_instance_autotermination_minutes

        self.client.execute_post_json(f"{self.base_uri}/edit", params=params)
        return self.get_by_id(instance_pool_id)

    def delete_by_id(self, instance_pool_id):
        self.client.execute_post_json(f"{self.base_uri}/delete", params={"instance_pool_id": instance_pool_id}, expected=[200, 404])
        return None

    def delete_by_name(self, name):
        pool = self.get_by_name(name)
        if pool is None: return None

        instance_pool_id = pool.get("instance_pool_id")
        return self.delete_by_id(instance_pool_id)
