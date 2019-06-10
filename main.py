import asyncio
import logging
import random
import string
import uuid

import attr
import functools


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)

@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)
    
    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'

# simulating an external publisher of events
async def publish(queue):
    choices = string.ascii_lowercase + string.digits
    while True:
        x = str(uuid.uuid4())
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=x, instance_name=f'cattle-{instance_name}')
        await queue.put(msg)
        logging.debug(f'Published messages {msg}')
        await asyncio.sleep(random.random()) 



async def handle_exception(coro, loop):
    try:
        await coro
    except Exception:
        logging.error('Caught Exception')
        loop.stop()

async def restart_host(msg):
    await asyncio.sleep(random.random())
    logging.info(f'Restarted {msg.hostname}')

async def save(msg):
    await asyncio.sleep(random.random())
    logging.info(f'Saved {msg} into database')


def cleanup(msg, fut):
    logging.info(f'Done Acked {msg}')


async def handle_message(msg):
    g_future = asyncio.gather(save(msg), restart_host(msg))
    callback = functools.partial(cleanup, msg)
    g_future.add_done_callback(callback)
    await g_future

async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        logging.info(f'Pulled {msg}')
        asyncio.create_task(handle_message(msg))

if __name__ == '__main__':
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    wrapper_publisher = handle_exception(publish(queue),loop)
    wrapper_consumer = handle_exception(consume(queue),loop)

    try:
        loop.create_task(wrapper_publisher)
        loop.create_task(wrapper_consumer)
        loop.run_forever()
    except KeyboardInterrupt:
        logging.debug('Interrupted')
    finally:
        logging.debug('Cleaning up')
        loop.stop()
