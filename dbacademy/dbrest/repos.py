from __future__ import annotations
from dbacademy.dbrest import DBAcademyRestClient

class ReposClient:
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def __call__(self) -> ReposClient:
        """Returns itself.  Provided for backwards compatibility."""
        return self

    def list(self) -> dict:
        return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/repos")

    def update(self, repo_id, branch) -> dict:
        return self.client.execute_patch_json(f"{self.client.endpoint}/api/2.0/repos/{repo_id}", {"branch": branch})

    def get(self, repo_id) -> dict:
        return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/repos/{repo_id}")

    def create(self, path:str, url:str, provider:str = "gitHub") -> dict:
        # /Repos/{folder}/{repo-name}
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/repos/", {
            "url": url,
            "provider": provider,
            "path": path
        })

    def delete(self, repo_id):
        return self.client.execute_delete(f"{self.client.endpoint}/api/2.0/repos/{repo_id}")
