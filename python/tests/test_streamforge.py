import pytest
import pytest_asyncio
import uuid
from httpx import AsyncClient, ASGITransport
from python.api.main import app, get_engine
from python.services.cpp_bridge import StreamForgeEngine
from python.services.redis_service import init_redis, close_redis, check_and_update_lsn
from python.database.connection import init_db_pool, close_db_pool, get_db_pool
from python.services.rabbitmq_service import init_rabbitmq, close_rabbitmq

@pytest.fixture(scope="module")
def cpp_engine():
    return StreamForgeEngine()

@pytest_asyncio.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

def test_cpp_engine_hash_partitioning(cpp_engine):
    res1 = cpp_engine.route_mutation(
        tenant_id="TENANT_STRIPE_PAYMENTS",
        table_name="charges",
        record_pk="ch_99999",
        lsn=1001,
        op_type=0, # INSERT
        total_partitions=16
    )
    assert 0 <= res1["partition_id"] < 16
    assert res1["routing_key"] == f"streamforge.TENANT_STRIPE_PAYMENTS.part_{res1['partition_id']}"

    # Determinism test: same inputs must yield exact same partition and hash
    res2 = cpp_engine.route_mutation(
        tenant_id="TENANT_STRIPE_PAYMENTS",
        table_name="charges",
        record_pk="ch_99999",
        lsn=1002,
        op_type=1, # UPDATE
        total_partitions=16
    )
    assert res1["partition_id"] == res2["partition_id"]
    assert res1["record_hash"] == res2["record_hash"]

def test_cpp_engine_batch_routing(cpp_engine):
    items = [
        {"tenant_id": f"t_{i}", "table_name": "orders", "record_pk": f"ord_{i}", "lsn_offset": 500 + i, "op_type": 0}
        for i in range(25)
    ]
    results = cpp_engine.batch_route(items, total_partitions=16)
    assert len(results) == 25
    for r in results:
        assert 0 <= r["partition_id"] < 16
        assert "streamforge.t_" in r["routing_key"]

def test_cpp_engine_lsn_monotonicity(cpp_engine):
    assert cpp_engine.is_valid_lsn(100, 101) is True
    assert cpp_engine.is_valid_lsn(100, 100) is False
    assert cpp_engine.is_valid_lsn(100, 99) is False

@pytest.mark.asyncio
async def test_redis_lsn_monotonicity():
    await init_redis()
    test_pk = f"test_rec_{uuid.uuid4().hex[:8]}"
    tenant = "TENANT_STRIPE_PAYMENTS"

    # First LSN 100 -> valid
    ok1 = await check_and_update_lsn(tenant, test_pk, 100)
    assert ok1 is True

    # Duplicate LSN 100 -> invalid (out of order / duplicate)
    ok2 = await check_and_update_lsn(tenant, test_pk, 100)
    assert ok2 is False

    # Lower LSN 95 -> invalid
    ok3 = await check_and_update_lsn(tenant, test_pk, 95)
    assert ok3 is False

    # Higher LSN 105 -> valid
    ok4 = await check_and_update_lsn(tenant, test_pk, 105)
    assert ok4 is True
    await close_redis()

@pytest.mark.asyncio
async def test_api_health(async_client):
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "HEALTHY"
    assert "StreamForge CDC Gateway" in data["service"]

@pytest.mark.asyncio
async def test_api_list_tenants(async_client):
    resp = await async_client.get("/api/v1/tenants")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 3
    tenant_ids = [t["tenant_id"] for t in data]
    assert "TENANT_STRIPE_PAYMENTS" in tenant_ids
    assert "TENANT_SHOPIFY_STORE" in tenant_ids

@pytest.mark.asyncio
async def test_api_cdc_ingest_lifecycle(async_client):
    pk = f"ch_{uuid.uuid4().hex[:6]}"
    payload = {
        "tenant_id": "TENANT_STRIPE_PAYMENTS",
        "table_name": "charges",
        "op_type": "INSERT",
        "record_pk": pk,
        "payload": {"amount": 4999, "currency": "USD", "customer": "cus_123"},
        "lsn_offset": 5001
    }
    resp = await async_client.post("/api/v1/cdc/ingest", json=payload)
    assert resp.status_code == 201
    res_data = resp.json()
    assert res_data["type"] == "CDC_MUTATION"
    assert res_data["record_pk"] == pk
    assert 0 <= res_data["partition_id"] < 16

    # Ingesting same LSN must trigger 409 Conflict
    dup_resp = await async_client.post("/api/v1/cdc/ingest", json=payload)
    assert dup_resp.status_code == 409

    # Ingesting higher LSN for UPDATE succeeds
    payload["op_type"] = "UPDATE"
    payload["payload"]["status"] = "succeeded"
    payload["lsn_offset"] = 5002
    update_resp = await async_client.post("/api/v1/cdc/ingest", json=payload)
    assert update_resp.status_code == 201
    update_data = update_resp.json()
    assert update_data["op_type"] == "UPDATE"
    assert update_data["partition_id"] == res_data["partition_id"]

@pytest.mark.asyncio
async def test_api_cdc_batch_ingest(async_client):
    batch_items = []
    for i in range(10):
        pk = f"prod_{uuid.uuid4().hex[:6]}"
        batch_items.append({
            "tenant_id": "TENANT_SHOPIFY_STORE",
            "table_name": "products",
            "op_type": "INSERT",
            "record_pk": pk,
            "payload": {"sku": f"SKU-{i}", "price": 49.99 + i},
            "lsn_offset": 6000 + i
        })
    resp = await async_client.post("/api/v1/cdc/ingest/batch", json={"items": batch_items})
    assert resp.status_code == 201
    data = resp.json()
    assert data["ingested"] == 10
    assert len(data["partitions_routed"]) == 10

@pytest.mark.asyncio
async def test_api_warehouse_stats(async_client):
    resp = await async_client.get("/api/v1/warehouse/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_cdc_mutations_ingested" in data
    assert data["total_cdc_mutations_ingested"] > 0
    assert len(data["active_partitions"]) >= 3
    assert len(data["recent_events"]) > 0

def test_spark_cdc_compactor_import():
    from python.spark.spark_cdc_compactor import run_cdc_compaction, build_spark_session
    assert callable(run_cdc_compaction)
    assert callable(build_spark_session)
