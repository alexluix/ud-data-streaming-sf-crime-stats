import asyncio

from confluent_kafka import Consumer


async def consume():
    consumer = Consumer({
        'bootstrap.servers': 'PLAINTEXT://localhost:9097',
        'group.id': 'test-consumer',
    })

    consumer.subscribe(['police.calls'])

    while True:
        for message in consumer.consume():
            if message is not None:
                print(f'{message.value()}\n')

        await asyncio.sleep(1)


def starter():
    try:
        asyncio.run(consume())

    except KeyboardInterrupt:
        print("Stopping..")


if __name__ == '__main__':
    starter()
