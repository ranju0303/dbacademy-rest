from typing import Literal
from typing import Any

OnError = Literal["ignore", "raise", "return"]


class ApiClient:
    def __init__(self,
                 url: str,
                 *,
                 token: str = None,
                 user: str = None,
                 password: str = None,
                 authorization_header: str = None,
                 throttle: int = 0):
        """
        Create a Databricks REST API client.

        Args:
            url: The common base URL to the API endpoints.  e.g. https://workspace.cloud.databricks.com/API/
            token: The API authentication token.  Defaults to None.
            user: The authentication username.  Defaults to None.
            password: The authentication password.  Defaults to None.
            authorization_header: The header to use for authentication.
                By default, it's generated from the token or password.
        """
        import requests
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        if authorization_header:
            pass
        elif token is not None:
            authorization_header = 'Bearer ' + token
        elif user is not None and password is not None:
            import base64
            encoded_auth = (user + ":" + password).encode()
            authorization_header = "Basic " + base64.standard_b64encode(encoded_auth).decode()
        else:
            raise ValueError("Must specify one of token, password, or authorization_header")
        if not url.endswith("/"):
            url += "/"

        if throttle > 0:
            s = "" if throttle == 1 else "s"
            print(f"** WARNING ** Requests are being throttled by {throttle} second{s} per request.")

        self.url = url
        self.user = user
        self.throttle = throttle
        self.read_timeout = 300   # seconds
        self.connect_timeout = 5  # seconds
        self._last_request_timestamp = 0

        backoff_factor = self.connect_timeout
        retry = Retry(connect=Retry.BACKOFF_MAX / backoff_factor, backoff_factor=backoff_factor)
        self.session = requests.Session()
        self.session.headers = {'Authorization': authorization_header, 'Content-Type': 'text/json'}
        self.session.mount('http://', HTTPAdapter(max_retries=retry))
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

    def api_simple(self, http_method: str, endpoint_path: str, *, on_error: OnError = "raise", **data) -> Any:
        """
        Invoke the Databricks REST API.

        Args:
            http_method: 'GET', 'PUT', 'POST', or 'DELETE'
            endpoint_path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            on_error: 'raise', 'ignore', or 'return'.
                'raise'  means propagate the HTTPError (Default)
                'ignore' means return None
                'return' means return the error message as parsed json if possible, otherwise as text.
            **data: Payload to attach to the HTTP request.  GET requests encode as params, all others as json.

        Returns:
            The return value of the API call as parsed JSON.  If the result is invalid JSON then the
            result will be returned as plain text.

        Raises:
            DatabricksApiException: If the API returns a 4xx error an error and on_error='raise'.
            requests.HTTPError: If the API returns any other error and on_error='raise'.
        """
        return self.api(http_method, endpoint_path, data, on_error=on_error)

    def api(self, http_method: str, endpoint_path: str, data: dict = {}, *, on_error: OnError = "raise"):
        """
        Invoke the Databricks REST API.

        Args:
            http_method: 'GET', 'PUT', 'POST', or 'DELETE'
            endpoint_path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            data: Payload to attach to the HTTP request.  GET requests encode as params, all others as json.
            on_error: 'raise', 'ignore', or 'return'.
                'raise'  means propagate the HTTPError (Default)
                'ignore' means return None
                'return' means return the error message as parsed json if possible, otherwise as text.

        Returns:
            The return value of the API call as parsed JSON.  If the result is invalid JSON then the
            result will be returned as plain text.

        Raises:
            requests.HTTPError: If the API returns an error and on_error='raise'.
        """
        import requests
        import pprint
        import json
        self._throttle_calls()
        url = self.url + endpoint_path.lstrip("/")
        timeout = (self.connect_timeout, self.read_timeout)
        if http_method == 'GET':
            params = {k: str(v).lower() if isinstance(v, bool) else v for k, v in data.items()}
            response = self.session.request(http_method, url, params=params, timeout=timeout)
        else:
            response = self.session.request(http_method, url, data=json.dumps(data), timeout=timeout)
        try:
            if on_error.lower() in ["raise", "throw", "error"]:
                response.raise_for_status()
            elif on_error.lower() in ["ignore", "none"] and not (200 <= response.status_code < 300):
                return None
            elif on_error.lower() not in ["return", "return_error"]:
                raise ValueError("on_error argument must be one of 'raise', 'ignore' or 'return'")
        except requests.exceptions.HTTPError as e:
            message = e.args[0]
            try:
                reason = pprint.pformat(response.json(), indent=2)
            except ValueError:
                reason = response.text
            message += '\n Response from server: \n {}'.format(reason)
            args_list = list(e.args)
            args_list[0] = message
            e.args = tuple(args_list)
            if 400 <= response.status_code < 500:
                raise DatabricksApiException(http_exception=e)
            else:
                raise
        try:
            return response.json()
        except ValueError:
            return response.text

    def _throttle_calls(self):
        if self.throttle <= 0:
            return
        import time
        now = time.time()
        elapsed = now - self._last_request_timestamp
        sleep_seconds = self.throttle - elapsed
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        self._last_request_timestamp = time.time()


class DatabricksApiException(Exception):
    def __init__(self, message=None, http_code=None, http_exception=None):
        import json
        if http_exception:
            self.__cause__ = http_exception
            self.cause = http_exception
            try:
                self.body = json.loads(http_exception.response.text)
                self.message = self.body.get("message", self.body.get("error", http_exception.response.text))
                self.error_code = self.body.get("error_code", -1)
            except ValueError:
                self.body = http_exception.response.text
                self.message = self.body
                self.error_code = -1
            self.method = http_exception.request.method
            self.endpoint = http_exception.request.path_url
            self.http_code = http_exception.response.status_code
            self.request = http_exception.request
            self.response = http_exception.response
        else:
            self.__cause__ = None
            self.cause = None
            self.body = message
            self.method = None
            self.endpoint = None
            self.http_code = http_code
            self.error_code = -1
            self.request = None
            self.response = None
        if message:
            self.message = message
        self.args = (self.method, self.endpoint, self.http_code, self.error_code, self.message)

    def __repr__(self):
        return (f"DatabricksApiException(message={self.message!r}, "
                f"http_code={self.http_code!r}, "
                f"http_exception={self.__cause__!r})")

    def __str__(self):
        return repr(self)
