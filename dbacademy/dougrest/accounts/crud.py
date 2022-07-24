from dbacademy.dougrest.common import DatabricksApiException


class CRUD(object):
    def __init__(self,
                 accounts,
                 path: str,
                 prefix: str,
                 singular: str = None,
                 plural: str = None):
        super().__init__(self)
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

    def get_by_id(self, id: str):
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

    def create(self, **kwargs):
        raise DatabricksApiException("Not Implemented", 501)
