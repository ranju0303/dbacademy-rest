from dbacademy.dougrest.sql.endpoints import Endpoints


class Sql(object):
    def __init__(self, databricks):
        self.databricks = databricks
        self.endpoints = Endpoints(databricks)
