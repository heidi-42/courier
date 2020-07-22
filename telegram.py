from __future__ import annotations

import asyncio
from time import time
from heapq import heappop, heappush

import keyring
from heidi import delivery
from heidi.ex import ServerError
from heidi.util import init_redis, init_data_layer, fetch

TELEGRAM = 'telegram'
access_token = keyring.get_password(TELEGRAM, 'heidi')
SEND_MESSAGE_URL = f'https://api.telegram.org/bot{access_token}/sendMessage'
RATE_LIMIT_ERRORS = {
    'FLOOD_WAIT_': True,
    'SLOWMODE_WAIT_': False,
}


def evaluate(response):
    # i.e. on success or connection / server errors.
    if 'error_code' not in response:
        return False, False, 0, 0

    error = response['description']
    has_error, worth_trying, global_cd, user_cd = True, False, 0, 0

    for desciption, is_global in RATE_LIMIT_ERRORS.items():
        if error.startswith(desciption):
            worth_trying = True

            cd_value = int(error.split('_')[-1])
            if is_global:
                global_cd = cd_value
            else:
                user_cd = cd_value
    return has_error, worth_trying, global_cd, user_cd


async def process_batch(task, redis):
    history_key, history, contacts = task

    telegram_ids, telegram_to_heidi = set(), {}
    for heidi_id, values in contacts.items():
        for telegram_id in values:
            telegram_ids.add(telegram_id)
            telegram_to_heidi[telegram_id] = heidi_id

    delivered_to = []
    # Telegram does not provide bulk notifications API.
    queue = [(0, telegram_id, 0) for telegram_id in telegram_ids]
    while queue:
        # `heapq` is a binary min-heap implementation using the first tuple
        # item as a priority.
        try_count, telegram_id, user_lock = heappop(queue)
        if try_count > 5:
            continue

        delta = user_lock - time() if user_lock > 0 else 0
        if delta > 0:
            await asyncio.sleep(delta)

        payload = {
            'chat_id': telegram_id,
            'text': history.text,
        }

        # Otherwise we will get interpreter error if the try-except block
        # below has been hit.
        response = None
        try:
            response = await fetch('POST', SEND_MESSAGE_URL, json=payload)
        except (ConnectionError, ServerError):
            # Giving the access network / Telegram API some time to recover.
            await asyncio.sleep(1)

        # Telegram implements both per user and per application cooldowns.
        has_error, worth_trying, global_cd, user_cd = evaluate(response)
        if not has_error:
            delivered_to.append(telegram_to_heidi[telegram_id])

        if not worth_trying:
            continue

        if global_cd > 0:
            await asyncio.sleep(global_cd)

        user_lock = time() + user_cd if user_cd > 0 else 0
        heappush(queue, (try_count + 1, telegram_id, user_lock))

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
