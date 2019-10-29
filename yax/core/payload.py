from pyshard.core.client import ClientABC, Serialyzer, ClientError
from pyshard.core.connect import AsyncTCPConnection


# TODO: fix this hell in pyshard lib
class AsyncClientBase(ClientABC):
    def __init__(self, host, port, transport_class=AsyncTCPConnection,
                 serialyzer=Serialyzer, **conn_kwargs):
        self.addr = (host, port)
        self._serialyzer = serialyzer
        self._transport = transport_class(host, port, **conn_kwargs)
        self._transport.connect()

    def _serialize(self, payload):
        return self._serialyzer.dump(payload)

    def _deserialize(self, response):
        return self._serialyzer.load(response)

    async def _execute(self, method, *args, **kwargs):
        payload = {'endpoint': method,
                   'args': args,
                   'kwargs': kwargs}

        await self._transport.send(self._serialize(payload))
        return self._handle_response(self._deserialize(await self._transport.recv()))

    def _handle_response(self, response):
        if response['type'] == 'error':
            err = response['message']
            raise ClientError(f'Couldn\'t execute: {err}')

        return response['message']

    def getsockname(self):
        return self._transport.getsockname()

    def close(self) -> None:
        self._transport.close()


class AsyncMasterClient(AsyncClientBase):
    def get_shard(self, index, key):
        return self._execute("get_shard", index, key)

    def get_map(self):
        return self._execute("get_map")

    def stat(self):
        return self._execute("stat")

    def create_index(self, index):
        return self._execute("create_index", index)


class Payload:
    def __init__(self, index, key, msg, master: AsyncMasterClient):
        self.index = index
        self.key = key
        self.msg = msg
        self._master = master
        self._hash = None

    async def hash(self):
        if not self._hash:
            ret = await self._master.get_shard(self.index, self.key)
            self._hash = tuple(ret[1])
        return self._hash
