import asyncio

from yax.worker.server import Server
from yax.core.payload import Payload, AsyncMasterClient


class App:
    def __init__(self, host, port, bootstrap_server):
        self._loop = asyncio.get_event_loop()
        self._shard_master = AsyncMasterClient(*bootstrap_server)
        payload_factory = lambda index, key, body: Payload(index, key, body, self._shard_master)
        self._server = Server(
            host, port, buffer_size=1024, payload_factory=payload_factory, loop=self._loop
        )

    def run(self):
        # self._loop.run_until_complete(self._shard_master.create_index("test_index"))  # TODO: add console command in pyshard
        self._loop.run_until_complete(self._server.run())


app = App("localhost", 9613, ("localhost", 9192))
app.run()
