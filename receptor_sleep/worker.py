import asyncio
import json
import logging

logger = logging.getLogger(__name__)


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
        logger.debug(
            f"Going to sleep for {duration} seconds, "
            f"iteration {idx + 1} of {repeat})"
        )
        await asyncio.sleep(duration)
        idx += 1
        await queue.put("iteration {}".format(idx))
    queue.done = True


def configure_logger():
    receptor_logger = logging.getLogger('receptor')
    logger.setLevel(receptor_logger.level)
    for handler in receptor_logger.handlers:
        logger.addHandler(handler)


def execute(message, config):
    configure_logger()
    loop = asyncio.get_event_loop()
    queue = ResponseQueue(loop=loop)
    try:
        payload = json.loads(message.raw_payload)
    except json.JSONDecodeError as err:
        logger.exception(err)
        raise
    logger.debug(f"Parsed payload: {payload}")
    loop.create_task(request(queue,
                             payload.pop("duration", 30),
                             payload.pop("repeat", 1)))
    return queue
