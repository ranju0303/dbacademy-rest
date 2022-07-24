from dbacademy.dougrest.accounts.crud import CRUD


class LogDeliveryConfigurations(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/log-delivery", "config")
