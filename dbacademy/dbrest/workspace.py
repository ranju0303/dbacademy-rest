from typing import Union
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class WorkspaceClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def ls(self, path, recursive=False, object_types=["NOTEBOOK"]):
        if not recursive:
            try:
                results = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/workspace/list?path={path}", expected=[200, 404])
                if results is None:
                    return None
                else:
                    return results.get("objects", [])

            except Exception as e:
                raise Exception(f"Unexpected exception listing {path}") from e
        else:
            entities = []
            queue = self.ls(path)
            
            if queue is None:
                return None

            while len(queue) > 0:
                next = queue.pop()
                object_type = next["object_type"]
                if object_type in object_types:
                    entities.append(next)
                elif object_type == "DIRECTORY":
                    result = self.ls(next["path"])
                    if result is not None: queue.extend(result)

            return entities

    def mkdirs(self, path) -> dict:
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/mkdirs", {"path": path})

    def delete_path(self, path) -> dict:
        payload = {"path": path, "recursive": True}
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/delete", payload, expected=[200, 404])

    def import_html_file(self, html_path: str, content: str, overwrite=True) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": html_path,
            "language": "PYTHON",
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def import_notebook(self, language: str, notebook_path: str, content: str, overwrite=True) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": notebook_path,
            "language": language,
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def export_notebook(self, notebook_path) -> str:
        from urllib.parse import urlencode
        params = urlencode({
            "path": notebook_path,
            "direct_download": "true"
        })
        return self.client.execute_get(f"{self.client.endpoint}/api/2.0/workspace/export?{params}").text

    def get_status(self, notebook_path) -> Union[None, dict]:
        from urllib.parse import urlencode
        params = urlencode({
            "path": notebook_path
        })
        response = self.client.execute_get(f"{self.client.endpoint}/api/2.0/workspace/get-status?{params}", expected=[200, 404])
        if response.status_code == 404:
            return None
        else:
            assert response.status_code == 200, f"({response.status_code}): {response.text}"
            return response.json()
