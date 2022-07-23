from dbacademy.dougrest.workspace import DatabricksApi
from dbacademy.dougrest.common import *


class AccountsApi(object):
    class CRUD(object):
        def __init__(self, accounts, path, prefix, *, singular=None, plural=None):
            self.accounts = accounts
            self.path = path
            self.prefix = prefix
            self.singular = singular or self.prefix
            self.plural = plural or self.singular + "s"
            # Update doc strings, replacing placeholders with actual values.
            cls = type(self)
            methods = [attr for attr in dir(cls) if not attr.startswith("__") and callable(getattr(cls, attr))]
            for name in methods:
                m = getattr(cls, name)
                if isinstance(m.__doc__, str):
                    m.__doc__ = m.__doc__.format(prefix=prefix, singular=singular, plural=plural)

        def list(self):
            """Returns a list of all {plural}."""
            return self.accounts.api("GET", f"{self.path}")

        def list_names(self):
            """Returns a list the names of all {plural}."""
            return [item[f"{self.prefix}_name"] for item in self.list()]

        def create(self, unimplemented):
            """Not Implemented."""
            raise DatabricksApiException("Not Implemented", 501)

        def get_by_id(self, id):
            """Returns the {singular} with the given unique {prefix}_id."""
            return self.accounts.api("GET", f"{self.path}/{id}")

        def get_by_name(self, name):
            """Returns the first {singular} found that with the given {prefix}_name.  Raises exception if not found."""
            result = next((item for item in self.list() if item[f"{self.prefix}_name"] == name), None)
            if result is None:
                raise DatabricksApiException(f"{self.singular} with name '{name}' not found", 404)
            return result

        def delete(self, item):
            """Deletes the provided {singular}."""
            id = item[f"{self.prefix}_id"]
            return self.delete_by_id(id)

        def delete_by_id(self, id):
            """Deletes the {singular} with the given id."""
            return self.accounts.api("DELETE", f"{self.path}/{id}")

        def delete_by_name(self, item_name):
            """
            Deletes the first {singular} with the given name.
            Raises an exception if no workspaces have the provided name.
            """
            item = self.get_by_name(item_name)
            return self.delete(item)

    class Credentials(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/credentials", "credentials", singular="credential")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class StorageConfigurations(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/storage-configurations", "storage_configuration")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class NetworkConfigurations(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/networks", "network")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class CustomerManagedKeys(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/customer-managed-keys", "customer_managed_key")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

        def history(self):
            """Get a list of records of how key configurations were associated with workspaces."""
            return self.accounts.api("GET", "/customer-managed-key-history")

    class LogDeliveryConfigurations(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/log-delivery", "config")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class PrivateAccessSettings(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/private-access-settings", "private_access_settings",
                                      plural="private_access_settingses")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class VpcEndpoints(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/vpc-endpoints", "vpc_endpoint")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class Budgets(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/budget", "budget")

        def create(self, unimplemented):
            raise DatabricksApiException("Not Implemented", 501)

    class Usage(object):
        def __init__(self, accounts):
            self.accounts = accounts

        def download(self, start_month, end_month, personal_data=False):
            return self.accounts.api("GET", "/usage/download", data={
                "start_month": start_month,
                "end_month": end_month,
                "personal_data": personal_data,
            })["message"]

    class Workspace(DatabricksApi):
        def __init__(self, data_dict, accounts_api):
            hostname = data_dict.get("deployment_name")
            auth = accounts_api.session.headers["Authorization"]
            self.accounts = accounts_api
            self.user = accounts_api.user
            super(AccountsApi.Workspace, self).__init__(hostname + ".cloud.databricks.com", user=self.user,
                                                        authorization_header=auth)
            self.update(data_dict)

        def wait_until_ready(self):
            while self["workspace_status"] == "PROVISIONING":
                workspace_id = self["workspace_id"]
                data = self.accounts.workspaces.get_by_id(workspace_id)
                self.update(data)
                if self["workspace_status"] == "PROVISIONING":
                    import time
                    time.sleep(15)

        def api_raw(self, method, path, data={}):
            self.wait_until_ready()
            return super().api_raw(method, path, data)

    class Workspaces(CRUD):
        def __init__(self, accounts):
            AccountsApi.CRUD.__init__(self, accounts, "/workspaces", "workspace")

        def list(self):
            """Returns a list of all {plural}."""
            workspaces = self.accounts.api("GET", f"{self.path}")
            #         return workspaces
            return [AccountsApi.Workspace(w, self.accounts) for w in workspaces]

        def get_by_id(self, id):
            """Returns the {singular} with the given unique {prefix}_id."""
            result = self.accounts.api("GET", f"{self.path}/{id}")
            return AccountsApi.Workspace(result, self.accounts)

        def get_by_deployment_name(self, name):
            """
            Returns the first {singular} found that with the given deployment_name.
            Raises exception if not found.
            """
            result = next((item for item in self.list() if item["deployment_name"] == name), None)
            if result is None:
                raise DatabricksApiException(f"{self.singular} with deployment_name '{name}' not found", 404)
            return result

        def create(self, workspace_name, *, deployment_name=None, region, pricing_tier=None,
                   credentials=None, credentials_id=None, credentials_name=None,
                   storage_configuration=None, storage_configuration_id=None, storage_configuration_name=None,
                   network=None, network_id=None, network_name=None,
                   private_access_settings=None, private_access_settings_id=None, private_access_settings_name=None,
                   services_encryption_key=None, services_encryption_key_id=None, services_encryption_key_name=None,
                   storage_encryption_key=None, storage_encryption_key_id=None, storage_encryption_key_name=None,
                   ):

            if credentials_id:
                pass
            elif credentials:
                credentials_id = credentials[f"credentials_id"]
            elif credentials_name:
                credentials_id = self.accounts.credentials.get_by_name(credentials_name)["credentials_id"]
            else:
                raise DatabricksApiException("Must provide one of credentials, credentials_id, or credentials_name")

            if storage_configuration_id:
                pass
            elif storage_configuration:
                storage_configuration_id = storage_configuration[f"storage_configuration_id"]
            elif storage_configuration_name:
                storage_configuration_id = self.accounts.storage.get_by_name(storage_configuration_name)[
                    "storage_configuration_id"]
            else:
                raise DatabricksApiException("Must provide one of credentials, credentials_id, or credentials_name")

            if network_id:
                pass
            elif network:
                network_id = network[f"network_id"]
            elif network_name:
                network_id = self.accounts.networks.get_by_name(network_name)["network_id"]

            if private_access_settings_id:
                pass
            elif private_access_settings:
                private_access_settings_id = private_access_settings[f"private_access_settings_id"]
            elif private_access_settings_name:
                private_access_settings_id = self.accounts.private_access.get_by_name(private_access_settings_name)[
                    "private_access_settings_id"]

            if services_encryption_key_id:
                pass
            elif services_encryption_key:
                services_encryption_key_id = services_encryption_key[f"customer_managed_key_id"]
            elif services_encryption_key_name:
                services_encryption_key_id = self.accounts.keys.get_by_name(services_encryption_key_name)[
                    "customer_managed_key_id"]

            if storage_encryption_key_id:
                pass
            elif storage_encryption_key:
                storage_encryption_key_id = storage_encryption_key[f"customer_managed_key_id"]
            elif storage_encryption_key_name:
                storage_encryption_key_id = self.accounts.keys.get_by_name(storage_encryption_key_name)[
                    "customer_managed_key_id"]

            spec = {
                "workspace_name": workspace_name,
                "deployment_name": deployment_name,
                "aws_region": region,
                "pricing_tier": pricing_tier,
                "credentials_id": credentials_id,
                "storage_configuration_id": storage_configuration_id,
                "network_id": network_id,
                "private_access_settings_id": private_access_settings_id,
                "managed_services_customer_managed_key_id": services_encryption_key_id,
                "storage_customer_managed_key_id": storage_encryption_key_id,
            }
            for key, value in list(spec.items()):
                if value is None or value == "":
                    del spec[key]

            result = self.accounts.api("POST", f"/workspaces", data=spec)
            return AccountsApi.Workspace(result, self.accounts)

        def update(self, item):
            """Updates (PATCH) the {singular} with new specified values."""
            return self.accounts.api("PATCH", f"{self.path}/{id}", **item)

    def __init__(self, account_id, user, password):
        import requests
        import base64
        encoded_auth = (user + ":" + password).encode()
        user_header_data = "Basic " + base64.standard_b64encode(encoded_auth).decode()
        self.user = user
        self.account_id = account_id
        self.url = f'https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}'
        self.session = requests.Session()
        self.session.headers = {'Authorization': user_header_data, 'Content-Type': 'text/json'}

        #     if not hasattr(type(self), "WorkspacesApi"):
        #       type(self).WorkspacesApi = Workspaces

        self.credentials = AccountsApi.Credentials(self)
        self.storage = AccountsApi.StorageConfigurations(self)
        self.networks = AccountsApi.NetworkConfigurations(self)
        self.keys = AccountsApi.CustomerManagedKeys(self)
        self.logs = AccountsApi.LogDeliveryConfigurations(self)
        self.vpc = AccountsApi.VpcEndpoints(self)
        self.budgets = AccountsApi.Budgets(self)
        self.workspaces = AccountsApi.Workspaces(self)
        self.private_access = AccountsApi.PrivateAccessSettings(self)

    def api(self, method, path, data={}):
        from json import JSONDecodeError
        response = self.api_raw(method, path, data)
        try:
            return response.json()
        except JSONDecodeError as e:
            e2 = DatabricksApiException(e.msg, 500)
            e2.__cause__ = e
            e2.cause = e
            e2.response = response

    def api_raw(self, method, path, data={}):
        import requests, pprint, json
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
