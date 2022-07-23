import typing


class DBAcademyRestClient:
  
    def __init__(self, token: str = None, endpoint: str = None, throttle: int = 0):
        """
        Creates a new instance of the Databricks Academy Rest Client.
            :param endpoint: The target endpoint or the notebook's default endpoint when None.
            :param token: The corresponding user's access token or the notebook's access token when None.
            :param throttle: The number of seconds to block before executing the next request.
        """

        import requests
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        self.throttle = throttle

        self.read_timeout = 300   # seconds
        self.connect_timeout = 5  # seconds

        backoff_factor = self.connect_timeout
        retry = Retry(connect=Retry.BACKOFF_MAX / backoff_factor, backoff_factor=backoff_factor)

        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

        if endpoint is not None:
            self.endpoint = endpoint
        else:
            from dbacademy import dbgems
            self.endpoint = dbgems.get_notebooks_api_endpoint()

        if token is not None:
            self.token = token
        else:
            from dbacademy import dbgems
            self.token = dbgems.get_notebooks_api_token()

        if self.throttle > 0:
            s = "" if self.throttle == 1 else "s"
            print(f"** WARNING ** Requests are being throttled by {self.throttle} second{s} per request.")

        from dbacademy.dbrest.clusters import ClustersClient
        self.clusters = ClustersClient(self)

        from dbacademy.dbrest.cluster_policies import ClusterPolicyClient
        self.cluster_policies = ClusterPolicyClient(self)

        from dbacademy.dbrest.jobs import JobsClient
        self.jobs = JobsClient(self)

        from dbacademy.dbrest.permissions import PermissionsClient
        self.permissions = PermissionsClient(self)

        from dbacademy.dbrest.pipelines import PipelinesClient
        self.pipelines = PipelinesClient(self)

        from dbacademy.dbrest.repos import ReposClient
        self.repos = ReposClient(self)

        from dbacademy.dbrest.runs import RunsClient
        self.runs = RunsClient(self)

        from dbacademy.dbrest.scim import ScimClient
        self.scim = ScimClient(self)

        from dbacademy.dbrest.sql import SqlClient
        self.sql = SqlClient(self)

        from dbacademy.dbrest.tokens import TokensClient
        self.tokens = TokensClient(self)

        from dbacademy.dbrest.token_management import TokenManagementClient
        self.token_management = TokenManagementClient(self)

        from dbacademy.dbrest.uc import UcClient
        self.uc = UcClient(self)

        from dbacademy.dbrest.workspace import WorkspaceClient
        self.workspace = WorkspaceClient(self)

    def help(self):
        methods = [func for func in dir(self) if callable(getattr(self, func)) and not func.startswith("__")]
        for method in methods:
            print(f"{method}()")

    def throttle_calls(self):
        import time
        time.sleep(self.throttle)

    def execute_patch_json(self, url: str, params: dict, expected=200) -> dict:
        return self.execute_patch(url, params, expected).json()

    def execute_patch(self, url: str, params: dict, expected=200):
        import json
        expected = self.expected_to_list(expected)

        response = self.session.patch(url, headers={"Authorization": "Bearer " + self.token}, data=json.dumps(params), timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_post_json(self, url: str, params: dict, expected=200) -> dict:
        return self.execute_post(url, params, expected).json()

    def execute_post(self, url: str, params: dict, expected=200):
        import json
        expected = self.expected_to_list(expected)

        response = self.session.post(url, headers={"Authorization": "Bearer " + self.token}, data=json.dumps(params), timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_put_json(self, url: str, params: dict, expected=200) -> dict:
        return self.execute_put(url, params, expected).json()

    def execute_put(self, url: str, params: dict, expected=200):
        import json
        expected = self.expected_to_list(expected)

        response = self.session.put(url, headers={"Authorization": "Bearer " + self.token}, data=json.dumps(params), timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_get_json(self, url: str, expected=200) -> typing.Union[dict, None]:
        response = self.execute_get(url, expected)

        if response.status_code == 200:
            return response.json()
        else:  # For example, expected includes 404
            return None

    def execute_get(self, url: str, expected=200):
        expected = self.expected_to_list(expected)

        response = self.session.get(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_delete_json(self, url: str, expected=(200, 404)) -> dict:
        response = self.execute_delete(url, expected)
        return response.json()

    def execute_delete(self, url: str, expected=(200, 404)):
        expected = self.expected_to_list(expected)

        response = self.session.delete(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    @staticmethod
    def expected_to_list(expected) -> list:
        if type(expected) == str: expected = int(expected)
        if type(expected) == int: expected = [expected]
        if type(expected) == tuple: expected = [expected]
        assert type(expected) == list, f"The parameter was expected to be of type str, int, tuple or list, found {type(expected)}"
        return expected
