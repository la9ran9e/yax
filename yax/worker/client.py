from pyshard.core.client import ClientBase


class Client(ClientBase):
    def apply(self, payload):
        return self._execute("apply", payload)
