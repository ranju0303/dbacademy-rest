from dbacademy.rest.common import ApiContainer


class Users(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def list(self):
        return self.databricks.api("GET", "2.0/preview/scim/v2/Users").get("Resources", [])

    def list_usernames(self):
        return sorted([u["userName"] for u in self.list()])

    def list_by_username(self):
        return {u["userName"]: u for u in self.list()}

    def get_by_id(self, id):
        return self.databricks.api("GET", f"2.0/preview/scim/v2/Users/{id}")

    def get_by_username(self, username):
        for u in self.list():
            if u["username"] == username:
                return u

    def update(self, user):
        id = user["id"]
        return self.databricks.api("PATCH", f"2.0/preview/scim/v2/Users/{id}", data=user)

    def create(self, username, allow_cluster_create=True):
        entitlements = []
        if allow_cluster_create:
            entitlements.append({"value": "allow-cluster-create"})
            entitlements.append({"value": "allow-instance-pool-create"})
        data = {
            "schemas": [
                "urn:ietf:params:scim:schemas:core:2.0:User"
            ],
            "userName": username,
            "entitlements": entitlements
        }
        return self.databricks.api("POST", "2.0/preview/scim/v2/Users", data=data)

    def delete_by_id(self, id):
        return self.databricks.api("DELETE", f"2.0/preview/scim/v2/Users/{id}")

    def delete_by_username(self, *usernames):
        user_id_map = {u['userName']: u['id'] for u in self.list()["Resources"]}
        for u in usernames:
            if u in user_id_map:
                self.delete_by_id(user_id_map[u])
