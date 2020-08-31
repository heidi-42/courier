from __future__ import annotations

import asyncio
from time import time
from heapq import heappop, heappush

from telethon.sync import TelegramClient
from telethon import errors as mt_errors

import keyring
from heidi import delivery
from heidi.util import init_redis, init_data_layer

TELEGRAM = 'telegram'
bot_token = keyring.get_password(f'{TELEGRAM}-token', 'heidi')
api_hash = keyring.get_password(f'{TELEGRAM}-api-hash', 'heidi')
api_id = keyring.get_password(f'{TELEGRAM}-api-id', 'heidi')

WAIT_THRESHOLD = 60

client = TelegramClient(
    None,
    api_id,
    api_hash,
    # Server [>=500] errors only.
    request_retries=10,
    # `telethon` will sleep accordingly for
    # X | 0 < FLOOD_WAIT_X <= WAIT_THRESHOLD
    # and raise `mt_errors.FloodWaitError` otherwise.
    flood_sleep_threshold=WAIT_THRESHOLD,
).start(bot_token=bot_token)


async def process_batch(task, redis):
    history_key, history, contacts = task

    telegram_ids, telegram_to_heidi = set(), {}
    for heidi_id, values in contacts.items():
        # Heidi stores contact values as strings.
        for telegram_id in map(int, values):
            telegram_ids.add(telegram_id)
            telegram_to_heidi[telegram_id] = heidi_id

    delivered_to = []

    # Telegram does not provide bulk notifications API.
    queue = [(0, telegram_id, 0) for telegram_id in telegram_ids]
    while queue:
        user_lock, telegram_id, try_count = heappop(queue)
        if try_count > 5:
            continue

        delta = user_lock - time() if user_lock > 0 else 0
        if delta > 0:
            await asyncio.sleep(delta)

        try:
            await client.send_message(telegram_id, history.text)
            delivered_to.append(telegram_to_heidi[telegram_id])
            continue
        except mt_errors.SlowModeWaitError as user_cooldown:
            user_lock = time() + user_cooldown.seconds
            if user_lock > WAIT_THRESHOLD:
                continue
        except mt_errors.FloodWaitError:
            break  # @see `client` initialization
        except OSError:
            # Giving the access network some time to recover.
            await asyncio.sleep(1)

        heappush(queue, (user_lock, telegram_id, try_count + 1))

    await delivery.update_status(history_key, TELEGRAM, delivered_to, redis)


async def main():
    app = {}
    await init_redis(app)
    await init_data_layer(app)
    redis = app['redis']

    async for task in delivery.task_stream(TELEGRAM, redis):
        asyncio.create_task(process_batch(task, redis))


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == '__main__':
    run()
