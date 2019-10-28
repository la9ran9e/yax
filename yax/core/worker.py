import asyncio


class Payload:
    # TODO: better implementation required
    def __init__(self, msg):
        self.msg = msg

    @property
    def id(self):
        return self.msg["id"]


class _Queue(asyncio.Queue):
    @property
    def unfinished_tasks(self):
        return self._unfinished_tasks


class Worker:
    def __init__(self, id, on_apply, on_done_payload, maxsize=1, loop=None):
        self.id = id
        self.maxsize = maxsize
        self._tasks = 0
        self._loop = loop if loop else asyncio.get_event_loop()
        self._queue = _Queue(maxsize=maxsize)
        self._on_apply = on_apply
        self._on_done_payload = on_done_payload

    def full(self):
        return self._queue._unfinished_tasks == self.maxsize

    def empty(self):
        return self._queue._unfinished_tasks <= self.maxsize - 1

    def size(self):
        return self._queue.unfinished_tasks

    async def join(self):
        await self._queue.join()

    def apply(self, payload):
        self._queue.put_nowait(payload)
        self._on_apply(self)
        asyncio.ensure_future(self._do_apply())

    async def _do_apply(self):
        payload = await self._queue.get()
        await self._actual_apply(payload)
        self._queue.task_done()
        self._on_done_payload(self)

    async def _actual_apply(self, payload):
        print("started apply")
        print("Payload:", payload)
        await asyncio.sleep(2)
        print("done apply")

    def __repr__(self):
        return f"<Task id={self.id}>"


class Workers:
    def __init__(self, workers_ids: list, loop=None):
        self._loop = loop if loop else asyncio.get_event_loop()
        self._workers = {id: Worker(id, self._on_apply, self._on_done_payload, maxsize=1, loop=self._loop) for id in workers_ids}
        self._free_workers = len(self._workers)
        self._cond = asyncio.Condition(loop=self._loop)

    @property
    def full(self):
        return self._free_workers == 0

    async def apply(self, payload):
        async with self._cond:
            if self.full:
                await self._cond.wait()
            worker = self._get_worker(payload.id)
            worker.apply(payload)

    def _get_worker(self, id):
        return self._workers[id]

    def _on_apply(self, worker: Worker):
        print(worker, worker.size(), self._free_workers)
        if worker.full():
            self._free_workers -= 1

    def _on_done_payload(self, worker: Worker):
        if worker.empty():
            self._free_workers += 1
            asyncio.ensure_future(self._wakeup())

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    async def join(self):
        await asyncio.gather(*[w.join() for w in self._workers.values()], loop=self._loop)

    def stats(self):
        stat = dict()
        for worker in self._workers.values():
            stat[worker.id] = f"{(worker.size() / worker.maxsize) * 100}%"
        return stat


async def main(loop):
    workers_ids = [1, 2]
    workers = Workers(workers_ids, loop=loop)
    ploads1 = [Payload({"id": 1}), Payload({"id": 2})]
    ploads2 = [Payload({"id": 1}), Payload({"id": 2})]
    for pload in ploads1 + ploads2:
        print("before:", workers.stats())
        await asyncio.sleep(.5)
        await workers.apply(pload)
        print("after:", workers.stats())
    await workers.join()
    print("after:", workers.stats())


if __name__ == "__main__":
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main(event_loop))
