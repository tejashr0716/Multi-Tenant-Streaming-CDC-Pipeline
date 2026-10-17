import asyncio
import random
import time
import statistics
import uuid
from httpx import AsyncClient, ASGITransport
from python.api.main import app
from python.database.connection import close_db_pool
from python.services.redis_service import close_redis
from python.services.rabbitmq_service import close_rabbitmq

TOTAL_MUTATIONS = 1200
CONCURRENCY = 15
TENANTS = ["TENANT_STRIPE_PAYMENTS", "TENANT_SHOPIFY_STORE", "TENANT_AIRBNB_BOOKINGS"]
TABLES = ["orders", "charges", "reservations", "customers", "inventory"]

async def worker(client: AsyncClient, queue: asyncio.Queue, latencies: list):
    while True:
        try:
            payload = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        t0 = time.perf_counter()
        resp = await client.post("/api/v1/cdc/ingest", json=payload)
        t1 = time.perf_counter()
        if resp.status_code == 201:
            latencies.append((t1 - t0) * 1000.0)

async def run_stress_test():
    print("=================================================================")
    print("  StreamForge Multi-Tenant Streaming CDC Ingest Stress Test      ")
    print(f"  Target: {TOTAL_MUTATIONS} mutations | Concurrency: {CONCURRENCY} workers")
    print("=================================================================\n")

    queue = asyncio.Queue()
    base_lsn = int(time.time() * 1000)
    for i in range(TOTAL_MUTATIONS):
        tenant = random.choice(TENANTS)
        table = random.choice(TABLES)
        pk = f"rec_{i}_{uuid.uuid4().hex[:6]}"
        queue.put_nowait({
            "tenant_id": tenant,
            "table_name": table,
            "op_type": "INSERT" if i % 10 != 0 else "UPDATE",
            "record_pk": pk,
            "payload": {
                "id": pk,
                "amount": round(random.uniform(10.0, 500.0), 2),
                "status": "PROCESSED",
                "ts": time.time()
            },
            "lsn_offset": base_lsn + i
        })

    latencies = []
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(worker(client, queue, latencies)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time

    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

    latencies.sort()
    throughput = len(latencies) / total_time
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f">> Completed:            {len(latencies)} / {TOTAL_MUTATIONS} mutations")
    print(f">> Total Wall Time:      {total_time:.2f} seconds")
    print(f">> Sustained Throughput: {throughput:,.1f} mutations/sec")
    print(f">> Latency p50:          {p50:.2f} ms")
    print(f">> Latency p95:          {p95:.2f} ms")
    print(f">> Latency p99:          {p99:.2f} ms")
    print("=================================================================")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
