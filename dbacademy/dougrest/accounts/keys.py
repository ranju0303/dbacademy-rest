from dbacademy.dougrest.accounts.crud import CRUD


class CustomerManagedKeys(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/customer-managed-keys", "customer_managed_key")

    def history(self):
        """Get a list of records of how key configurations were associated with workspaces."""
        return self.accounts.api("GET", "/customer-managed-key-history")
