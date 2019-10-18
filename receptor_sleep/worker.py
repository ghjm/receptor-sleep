import asyncio
import json


class ResponseQueue(asyncio.Queue):

    def __init__(self, *args, **kwargs):
        self.done = False
        super().__init__(*args, **kwargs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.done:
            raise StopAsyncIteration
        return await self.get()


async def request(queue, duration, repeat):
    idx = 0
    while idx < repeat:
        await asyncio.sleep(duration)
        idx += 1
        await queue.put("iteration {}".format(idx))
    queue.done = True


def execute(message):
    loop = asyncio.get_event_loop()
    queue = ResponseQueue(loop=loop)
    payload = json.loads(message.raw_payload)
    loop.create_task(request(queue,
                             payload.pop("duration", 30),
                             payload.pop("repeat", 1)))
    return queue
