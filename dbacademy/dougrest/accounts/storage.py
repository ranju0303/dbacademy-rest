from dbacademy.dougrest.accounts.crud import CRUD


class StorageConfigurations(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/storage-configurations", "storage_configuration")
