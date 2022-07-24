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

    def api(self, method, path, data={}):
        from json import JSONDecodeError
        response = self._api_raw(method, path, data)
        try:
            return response.json()
        except JSONDecodeError as e:
            e2 = DatabricksApiException(e.msg, 500)
            e2.__cause__ = e
            e2.cause = e
            e2.response = response

    def _api_raw(self, method, path, data={}):
        import requests
        import pprint
        import json
        if method == 'GET':
            translated_data = {k: DatabricksApi._translate_boolean_to_query_param(data[k]) for k in data}
            resp = self.session.request(method, self.url + path, params=translated_data)
        else:
            resp = self.session.request(method, self.url + path, data=json.dumps(data))
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            message = e.args[0]
            try:
                reason = pprint.pformat(json.loads(resp.text), indent=2)
                message += '\n Response from server: \n {}'.format(reason)
            except ValueError:
                pass
            if 400 <= e.response.status_code < 500:
                raise DatabricksApiException(http_exception=e)
            else:
                raise requests.exceptions.HTTPError(message, response=e.response)
        return resp
