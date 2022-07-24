from dbacademy.dougrest.accounts.crud import CRUD


class PrivateAccessSettings(CRUD):
    def __init__(self, accounts):
        # The unusual plural form private_access_settingses is intentional.
        super().__init__(accounts, "/private-access-settings", "private_access_settings", "private_access_settingses")
