from dbacademy.dougrest.mlflow.models import RegisteredModels
from dbacademy.dougrest.mlflow.versions import ModelVersions


class MLFlow(object):
    def __init__(self, databricks):
        self.databricks = databricks
        self.registered_models = RegisteredModels(databricks)
        self.models = RegisteredModels(databricks)
        self.model_versions = ModelVersions(databricks)
        self.versions = ModelVersions(databricks)
