from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class MLflowClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/preview/mlflow/"

    def get_endpoint_status(self, model_name):
        url = f"{self.base_uri}/endpoints/get-status?registered_model_name={model_name}"
        return self.client.execute_get_json(url).get("endpoint_status")

    def wait_for_endpoint(self, model_name, delay_seconds=10):
        import time
        while True:
            endpoint_status = self.get_endpoint_status(model_name)
            state = endpoint_status.get("state")
            if state == "ENDPOINT_STATE_READY":
                print("-"*80)
                return
            else:
                print(f"Endpoint not ready ({state}), waiting {delay_seconds} seconds")
                time.sleep(delay_seconds)  # Wait N seconds
