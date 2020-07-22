from __future__ import annotations

import asyncio

from more_itertools import chunked, flatten

import keyring
from heidi import delivery
from heidi.ex import ServerError
from heidi.util import init_redis, init_data_layer, fetch

VK = 'vk'
API_VERSION = '5.120'
VK_EXECUTE_URL = 'https://api.vk.com/method/execute'
WORTH_RETRY_ERRORS = {
    1,  # Unknown error
    6,  # Rate/Quantity limits
    9,
    944,
    950,  # Timeout
    13,  # Runtime
}

access_token = keyring.get_password(VK, 'heidi')

SEND_SCRIPT_TEMPLATE = \
    '''var result = [];

    var sendMessageChunks = %s;
    var i = sendMessageChunks.length;
    while (i > 0) {
        i = i - 1;
        
        result.push(API.messages.send({
            random_id: %d,
            message: '%s',
            user_ids: sendMessageChunks[i]
        }));
    }
    
    return result;'''


def compile_send_scripts(vk_ids, text, random_id):
    # 1. `execute` script may contain up to 25 API requests;
    # 2. `messages.send` request may contain up to 100 user ids.
    scripts = []
    for execute_chunk in chunked(vk_ids, 2500):
        send_chunks = list(chunked(execute_chunk, 100))
        scripts.append(SEND_SCRIPT_TEMPLATE %
                       (repr(send_chunks), random_id, text))
    return scripts


def evaluate(response):
    # i.e. connection / server error.
    if response is None:
        return True, True

    if 'error' in response:
        code = response['error']['error_code']
        return True, code in WORTH_RETRY_ERRORS

    return False, False


async def process_batch(task, redis):
    history_key, history, contacts = task

    vk_ids, vk_to_heidi = set(), {}
    for heidi_id, values in contacts.items():
        for vk_id in values:
            vk_ids.add(vk_id)
            # Heidi stores all contact values as strings and `messages.send`
            # method returns ints on success.
            vk_to_heidi[int(vk_id)] = heidi_id

    delivered_to = []

    # Users won't receive messages sharing the same `random_id`
    # more than once in a couple of hours.
    for script in compile_send_scripts(vk_ids, history.text, random_id=2**42):
        # Each `messages.send` response contains a list a previously sent
        # messages with the same `random_id`.
        try_count, response = 0, None

        while try_count < 6:
            try_count += 1

            try:
                params = {
                    'code': script,
                    'v': API_VERSION,
                    'access_token': access_token,
                }

                response = await fetch('GET', VK_EXECUTE_URL, params=params)
            except (ConnectionError, ServerError):
                # Giving the access network / VK API some time to recover.
                await asyncio.sleep(1)
                continue

            has_error, worth_trying = evaluate(response)
            if has_error:
                if not worth_trying:
                    break
                # TODO: Retry-After?
                await asyncio.sleep(1)
            else:
                delivered_to += [
                    vk_to_heidi[item['peer_id']]
                    # @see SEND_SCRIPT_TEMPLATE
                    for item in flatten(response['response'])
                    if 'error' not in item
                ]

                break

    await delivery.update_status(history_key, VK, delivered_to, redis)


async def main():
    app = {}
    await init_redis(app)
    await init_data_layer(app)
    redis = app['redis']

    async for task in delivery.task_stream(VK, redis):
        asyncio.create_task(process_batch(task, redis))


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == '__main__':
    run()
