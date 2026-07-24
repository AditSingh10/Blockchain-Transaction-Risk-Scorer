from __future__ import annotations

from prometheus_client import Gauge

consumer_lag = Gauge(
    "risk_kafka_consumer_lag",
    "Approximate records between a consumer position and the partition high watermark",
    ["consumer_group", "topic", "partition"],
)


async def observe_consumer_lag(consumer, consumer_group: str) -> None:
    for partition in consumer.assignment():
        high_watermark = consumer.highwater(partition)
        if high_watermark is None:
            continue
        position = await consumer.position(partition)
        consumer_lag.labels(
            consumer_group,
            partition.topic,
            str(partition.partition),
        ).set(max(high_watermark - position, 0))
