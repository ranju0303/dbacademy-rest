from dbacademy.dougrest.accounts.budgets import Budgets
from dbacademy.dougrest.accounts.credentials import Credentials
from dbacademy.dougrest.accounts.crud import CRUD
from dbacademy.dougrest.accounts.keys import CustomerManagedKeys
from dbacademy.dougrest.accounts.logs import LogDeliveryConfigurations
from dbacademy.dougrest.accounts.network import NetworkConfigurations
from dbacademy.dougrest.accounts.private_access import PrivateAccessSettings
from dbacademy.dougrest.accounts.storage import StorageConfigurations
from dbacademy.dougrest.accounts.vpc import VpcEndpoints
from dbacademy.dougrest.accounts.workspaces import Workspaces
from dbacademy.dougrest.client import DatabricksApi
from dbacademy.dougrest.common import *


class AccountsApi(ApiClient):
    def __init__(self, account_id, user, password):
        url = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}'
        super().__init__(url, user=user, password=password)
        self.user = user
        self.account_id = account_id
        self.credentials = Credentials(self)
        self.storage = StorageConfigurations(self)
        self.networks = NetworkConfigurations(self)
        self.keys = CustomerManagedKeys(self)
        self.logs = LogDeliveryConfigurations(self)
        self.vpc = VpcEndpoints(self)
        self.budgets = Budgets(self)
        self.workspaces = Workspaces(self)
        self.private_access = PrivateAccessSettings(self)
