# coding=utf-8

import logging, time
from async.async_kafka_server import AsyncProducer
from sync.sync_kafka_server import SyncProducer


def main():
    threads = {
        AsyncProducer(),
        SyncProducer()
    }

    for t in threads:
        print('abc')
        t.start()

    time.sleep(5000)


if __name__ == '__main__':
    # logging.basicConfig(
    #     format='%(asctime)s.%(msecs)s:%(thread)d:%(levelname)s:%(process)d:%(messages)s',
    #     level=logging.INFO
    # )
    main()
