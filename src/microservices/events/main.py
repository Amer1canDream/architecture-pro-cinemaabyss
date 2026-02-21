import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("events-service")

app = FastAPI(title="CinemaAbyss Events Service", version="1.0.0")

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cinemaabyss-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "events-service-group")

producer: Optional[AIOKafkaProducer] = None
consumer: Optional[AIOKafkaConsumer] = None
consumer_task: Optional[asyncio.Task] = None


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int


class UserEvent(BaseModel):
    user_id: int
    username: str
    action: str
    timestamp: str


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: str


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


async def ensure_topic():
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    await admin.start()
    try:
        topics = await admin.list_topics()
        if KAFKA_TOPIC in topics:
            log.info("Kafka topic exists: %s", KAFKA_TOPIC)
            return
        await admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
        log.info("Kafka topic created: %s", KAFKA_TOPIC)
    except Exception as e:
        log.warning("Topic ensure skipped/failed (%s): %s", KAFKA_TOPIC, e)
    finally:
        await admin.close()


async def start_kafka():
    global producer, consumer, consumer_task

    await ensure_topic()

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    log.info("Producer started (%s)", KAFKA_BOOTSTRAP)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    log.info("Consumer started: topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    consumer_task = asyncio.create_task(consume_loop())


async def stop_kafka():
    global producer, consumer, consumer_task

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except Exception:
            pass

    if consumer:
        await consumer.stop()
        consumer = None
        log.info("Consumer stopped")

    if producer:
        await producer.stop()
        producer = None
        log.info("Producer stopped")


async def consume_loop():
    assert consumer is not None
    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
            except Exception:
                log.warning("Bad message (not json): %r", msg.value)
                continue

            log.info(
                "EVENT CONSUMED | topic=%s partition=%s offset=%s key=%s value=%s",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.key.decode("utf-8") if msg.key else None,
                data,
            )
    except asyncio.CancelledError:
        return


async def produce(event_type: str, payload: dict):
    """Продюсим событие в Kafka."""
    if producer is None:
        raise RuntimeError("Producer not started")

    envelope = {
        "type": event_type,
        "created_at": now_iso(),
        "payload": payload,
    }

    await producer.send_and_wait(
        KAFKA_TOPIC,
        key=event_type.encode("utf-8"),
        value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
    )
    log.info("EVENT PRODUCED | type=%s payload=%s", event_type, payload)


@app.get("/api/events/health")
async def health():
    return {"status": True}


@app.post("/api/events/movie")
async def create_movie_event(payload: MovieEvent):
    await produce("Movie", payload.model_dump())
    return JSONResponse(status_code=201, content={"status": "success"})


@app.post("/api/events/user")
async def create_user_event(payload: UserEvent):
    await produce("User", payload.model_dump())
    return JSONResponse(status_code=201, content={"status": "success"})


@app.post("/api/events/payment")
async def create_payment_event(payload: PaymentEvent):
    await produce("Payment", payload.model_dump())
    return JSONResponse(status_code=201, content={"status": "success"})


@app.on_event("startup")
async def _startup():
    await start_kafka()


@app.on_event("shutdown")
async def _shutdown():
    await stop_kafka()
