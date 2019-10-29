from pyshard.core.client import ClientBase


class Client(ClientBase):
    def __init__(self, *args, index, **kwargs):
        self.index = index
        super().__init__(*args, **kwargs)

    def apply(self, key, payload):
        return self._execute("apply", self.index, key, payload)
