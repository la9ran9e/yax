from yax.core.worker import Workers

from pyshard.core.server import ServerBase


class Server(ServerBase):
    def __init__(self, *args, payload_factory, **kwargs):
        self._queue_class = Workers
        self._queue: Workers = None
        self._payload_factory = payload_factory
        super(Server, self).__init__(*args, **kwargs)

    async def run(self):
        self._queue = Workers(maxsize=1)
        await self._do_run()

    @ServerBase.endpoint("apply")
    async def apply(self, index, key, payload):
        print(index, key, payload)
        await self._queue.apply(self._payload_factory(index, key, payload))
        print(self._queue.stats())
