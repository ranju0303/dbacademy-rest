# Databricks notebook source
from dbacademy import dbgems
from dbacademy.dbrest import DBAcademyRestClient


class ClustersClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def get(self, cluster_id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")

    def get_current_spark_version(self):
        from dbacademy import dbgems
        cluster_id = dbgems.get_tags()["clusterId"]
        return self.get(cluster_id).get("spark_version", None)

    def get_current_instance_pool_id(self):
        from dbacademy import dbgems
        cluster_id = dbgems.get_tags()["clusterId"]
        return self.get(cluster_id).get("instance_pool_id", None)

    def get_current_node_type_id(self):
        from dbacademy import dbgems
        cluster_id = dbgems.get_tags()["clusterId"]
        return self.get(cluster_id).get("node_type_id", None)
