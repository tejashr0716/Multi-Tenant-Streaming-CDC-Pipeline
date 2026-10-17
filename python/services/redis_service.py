import json
import os
import redis.asyncio as redis
from typing import Optional, Dict, Any

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "7"))

redis_client: Optional[redis.Redis] = None

async def init_redis() -> redis.Redis:
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    return redis_client

async def get_redis() -> redis.Redis:
    global redis_client
    if redis_client is None:
        await init_redis()
    return redis_client

async def close_redis():
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

async def check_and_update_lsn(tenant_id: str, record_pk: str, incoming_lsn: int, ttl_seconds: int = 86400) -> bool:
    client = await get_redis()
    key = f"streamforge:dedup:{tenant_id}:{record_pk}"
    last_lsn = await client.get(key)
    if last_lsn is not None and int(last_lsn) >= incoming_lsn:
        return False # Stale or duplicate LSN
    await client.set(key, str(incoming_lsn), ex=ttl_seconds)
    return True

async def increment_ingest_counter(tenant_id: str):
    client = await get_redis()
    key = f"streamforge:stats:ingest:{tenant_id}"
    await client.incr(key)

async def publish_cdc_mutation(event: Dict[str, Any]):
    client = await get_redis()
    await client.publish("streamforge:cdc_stream", json.dumps(event))
