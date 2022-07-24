from dbacademy.dougrest.common import ApiClient


class DatabricksApi(dict, ApiClient):

    default_machine_types = {
        "AWS": "i3.xlarge",
        "Azure": "Standard_DS3_v2",
        "GCP": "n1-standard-4",
    }

    _default_client = None

    @classmethod
    def default_client(cls):
        if cls._default_client is None:
            import os
            import configparser
            for path in ('.databrickscfg', '~/.databrickscfg'):
                path = os.path.expanduser(path)
                if not os.path.exists(path):
                    continue
                config = configparser.ConfigParser()
                config.read(path)
                if 'DEFAULT' not in config:
                    print('No Default')
                    continue
                host = config['DEFAULT']['host'].rstrip("/")[8:]
                token = config['DEFAULT']['token']
                return DatabricksApi(host, token=token)
            cls._default_client = DatabricksApi()
        return cls._default_client

    def __init__(self, hostname=None, *, token=None, user=None, password=None, authorization_header=None, cloud="AWS",
                 deployment_name=None):
        from dbacademy.rest.dbgems_fix import dbgems
        if hostname:
            url = f'https://{hostname}/api/'
        else:
            url = dbgems.get_notebooks_api_endpoint() + "/api/"
        if not any((authorization_header, token, password)):
            token = dbgems.get_notebooks_api_token()
        super(dict, self).__init__(url, token=token, user=user, password=password,
                                   authorization_header=authorization_header)
        self["deployment_name"] = deployment_name
        self.cloud = cloud
        self.default_machine_type = DatabricksApi.default_machine_types[self.cloud]
        self.default_preloaded_versions = ["10.4.x-cpu-ml-scala2.12", "10.4.x-cpu-scala2.12"]
        self.default_spark_version = self.default_preloaded_versions[0]
        from dbacademy.dougrest.clusters import Clusters
        self.clusters = Clusters(self)
        from dbacademy.dougrest.groups import Groups
        self.groups = Groups(self)
        from dbacademy.dougrest.jobs import Jobs
        self.jobs = Jobs(self)
        from dbacademy.dougrest.mlflow import MLFlow
        self.mlflow = MLFlow(self)
        from dbacademy.dougrest.pools import Pools
        self.pools = Pools(self)
        from dbacademy.dougrest.repos import Repos
        self.repos = Repos(self)
        from dbacademy.dougrest.scim import SCIM, Users
        self.scim = SCIM(self)
        self.users = Users(self)
        from dbacademy.dougrest.sql import Sql
        self.sql = Sql(self)
        from dbacademy.dougrest.workspace import Workspace
        self.workspace = Workspace(self)

    # @property
    # def workspace_config(self):
    #     driver = getattr(getattr(sc._jvm, "com.databricks.backend.common.util.Project$Driver$"), "MODULE$")
    #     empty = sc._jvm.com.databricks.conf.Configs.empty()
    #     dbHome = sc._jvm.com.databricks.conf.StaticConf.DB_HOME()
    #     configFile = sc._jvm.com.databricks.conf.trusted.ProjectConf.loadLocalConfig(driver, empty, False, dbHome)
    #     driverConf = sc._jvm.com.databricks.backend.daemon.driver.DriverConf(configFile)
    #     return driverConf
    #
    # @property
    # def execution_context(self):
    #     return dbutils.entry_point.getDbutils().notebook().getContext()
    #
    # @property
    # def cloud_provider(self):
    #     return self.workspace_config.cloudProvider().get()
    #
    # @property
    # def cluster_id(self):
    #     return self.execution_context.clusterId().get()
