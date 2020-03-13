import json
import logging
import time

import receptor

logger = logging.getLogger(__name__)


def configure_logger():
    receptor_logger = logging.getLogger("receptor")
    logger.setLevel(receptor_logger.level)
    for handler in receptor_logger.handlers:
        logger.addHandler(handler)


@receptor.plugin_export(receptor.BYTES_PAYLOAD)
def execute(message, config, result_queue):
    configure_logger()
    try:
        payload = json.loads(message)
    except json.JSONDecodeError as err:
        logger.exception(err)
        raise
    logger.debug(f"Parsed payload: {payload}")

    duration = payload.pop("duration", 30)
    repeat = payload.pop("repeat", 1)
    ident = payload.pop("ident", "No ID")

    idx = 0
    while idx < repeat:
        logger.debug(
            f"Going to sleep for {duration} seconds, "
            f"iteration {idx + 1} of {repeat})"
        )
        time.sleep(duration)
        idx += 1
        result_queue.put("{}: iteration {}".format(ident, idx))
