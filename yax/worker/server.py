import asyncio

from yax.core.worker import Workers, Payload

from pyshard.core.server import ServerBase


class Server(ServerBase):
    def __init__(self, *args, **kwargs):
        self._queue_class = Workers
        self._queue: Workers = None
        super(Server, self).__init__(*args, **kwargs)

    def run(self):
        worker_ids = self._read_config()
        self._queue = Workers(worker_ids)
        self._loop.run_until_complete(self._do_run())

    @ServerBase.endpoint("apply")
    async def apply(self, payload):
        await self._queue.apply(Payload(payload))
        print(self._queue.stats())

    def _read_config(self):
        return [1, 2, 3]
