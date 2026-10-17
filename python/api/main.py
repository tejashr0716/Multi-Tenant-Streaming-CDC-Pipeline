import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from python.database.connection import init_db_pool, close_db_pool, get_db_pool
from python.services.cpp_bridge import StreamForgeEngine
from python.services.redis_service import (
    init_redis, close_redis, check_and_update_lsn, increment_ingest_counter, publish_cdc_mutation
)
from python.services.rabbitmq_service import init_rabbitmq, close_rabbitmq, publish_cdc_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("streamforge.api")

engine: Optional[StreamForgeEngine] = None

def get_engine() -> StreamForgeEngine:
    global engine
    if engine is None:
        engine = StreamForgeEngine()
    return engine

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing StreamForge Services...")
    await init_db_pool()
    await init_redis()
    await init_rabbitmq()
    get_engine()
    logger.info("StreamForge CDC Engine and distributed services ready.")
    yield
    logger.info("Shutting down StreamForge Services...")
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

app = FastAPI(
    title="StreamForge Multi-Tenant Streaming CDC Pipeline",
    version="1.0.0",
    description="High-Throughput Change Data Capture (CDC) Routing & Streaming Lakehouse Ingestion Engine",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class CDCMutationPayload(BaseModel):
    tenant_id: str
    table_name: str
    op_type: str = "INSERT" # INSERT, UPDATE, DELETE
    record_pk: str
    payload: Dict[str, Any]
    lsn_offset: int

class CDCBatchPayload(BaseModel):
    items: List[CDCMutationPayload]

@app.get("/health")
async def health_check():
    return {
        "status": "HEALTHY",
        "service": "StreamForge CDC Gateway",
        "engine": "C++ Hash Partitioning & LSN Deduplication Core Loaded",
        "active_websockets": len(ws_manager.active_connections)
    }

@app.get("/api/v1/tenants")
async def list_tenants():
    db = await get_db_pool()
    async with db.acquire() as conn:
        tenants = await conn.fetch("SELECT * FROM tenants ORDER BY tenant_id ASC;")
        output = []
        for t in tenants:
            tables = await conn.fetch("SELECT * FROM cdc_source_tables WHERE tenant_id = $1;", t["tenant_id"])
            event_count = await conn.fetchval(
                "SELECT COUNT(*) FROM cdc_mutation_events WHERE tenant_id = $1;", t["tenant_id"]
            )
            output.append({
                "tenant_id": t["tenant_id"],
                "organization_name": t["organization_name"],
                "tier": t["tier"],
                "cdc_stream_topic": t["cdc_stream_topic"],
                "status": t["status"],
                "registered_tables": [dict(tbl) for tbl in tables],
                "total_mutations": event_count
            })
        return output

@app.post("/api/v1/cdc/ingest", status_code=status.HTTP_201_CREATED)
async def ingest_mutation(payload: CDCMutationPayload):
    db = await get_db_pool()
    eng = get_engine()

    # 1. Deduplication via LSN Monotonicity Check in Redis
    is_valid = await check_and_update_lsn(payload.tenant_id, payload.record_pk, payload.lsn_offset)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Duplicate or out-of-order LSN offset {payload.lsn_offset} for key '{payload.record_pk}'."
        )

    # 2. C++ High-Speed Partition Routing
    op_code = 0 if payload.op_type == "INSERT" else (1 if payload.op_type == "UPDATE" else 2)
    routing = eng.route_mutation(
        tenant_id=payload.tenant_id,
        table_name=payload.table_name,
        record_pk=payload.record_pk,
        lsn=payload.lsn_offset,
        op_type=op_code,
        total_partitions=16
    )

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO cdc_mutation_events (tenant_id, table_name, op_type, record_pk, payload_json, lsn_offset, partition_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING event_id, ingested_at;
            """,
            payload.tenant_id, payload.table_name, payload.op_type, payload.record_pk,
            json.dumps(payload.payload), payload.lsn_offset, routing["partition_id"]
        )

        event = {
            "type": "CDC_MUTATION",
            "event_id": row["event_id"],
            "tenant_id": payload.tenant_id,
            "table_name": payload.table_name,
            "op_type": payload.op_type,
            "record_pk": payload.record_pk,
            "lsn_offset": payload.lsn_offset,
            "partition_id": routing["partition_id"],
            "routing_key": routing["routing_key"],
            "record_hash": routing["record_hash"],
            "ingested_at": row["ingested_at"].isoformat()
        }

        # Redis stats & RabbitMQ persistence
        await increment_ingest_counter(payload.tenant_id)
        await publish_cdc_mutation(event)
        await publish_cdc_event(routing["routing_key"], event)
        asyncio.create_task(ws_manager.broadcast(event))

        return event

@app.post("/api/v1/cdc/ingest/batch", status_code=status.HTTP_201_CREATED)
async def ingest_batch_mutations(payload: CDCBatchPayload):
    if not payload.items:
        return {"ingested": 0, "results": []}

    db = await get_db_pool()
    eng = get_engine()

    items_dict = [
        {
            "tenant_id": item.tenant_id,
            "table_name": item.table_name,
            "record_pk": item.record_pk,
            "lsn_offset": item.lsn_offset,
            "op_type": 0 if item.op_type == "INSERT" else (1 if item.op_type == "UPDATE" else 2)
        }
        for item in payload.items
    ]

    decisions = eng.batch_route(items_dict, total_partitions=16)

    records_to_insert = []
    for item, dec in zip(payload.items, decisions):
        records_to_insert.append((
            item.tenant_id, item.table_name, item.op_type, item.record_pk,
            json.dumps(item.payload), item.lsn_offset, dec["partition_id"]
        ))
        await check_and_update_lsn(item.tenant_id, item.record_pk, item.lsn_offset)

    async with db.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO cdc_mutation_events (tenant_id, table_name, op_type, record_pk, payload_json, lsn_offset, partition_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7);
            """,
            records_to_insert
        )

    return {
        "ingested": len(records_to_insert),
        "partitions_routed": [d["partition_id"] for d in decisions]
    }

@app.get("/api/v1/warehouse/stats")
async def get_warehouse_stats():
    db = await get_db_pool()
    async with db.acquire() as conn:
        partitions = await conn.fetch("SELECT * FROM warehouse_partitions ORDER BY partition_id ASC;")
        total_events = await conn.fetchval("SELECT COUNT(*) FROM cdc_mutation_events;")
        recent_mutations = await conn.fetch("SELECT * FROM cdc_mutation_events ORDER BY event_id DESC LIMIT 20;")

        return {
            "total_cdc_mutations_ingested": total_events,
            "active_partitions": [dict(p) for p in partitions],
            "recent_events": [
                {
                    "event_id": m["event_id"],
                    "tenant_id": m["tenant_id"],
                    "table_name": m["table_name"],
                    "op_type": m["op_type"],
                    "record_pk": m["record_pk"],
                    "lsn_offset": m["lsn_offset"],
                    "partition_id": m["partition_id"],
                    "ingested_at": m["ingested_at"].isoformat()
                }
                for m in recent_mutations
            ]
        }

@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        await websocket.send_json({
            "type": "CONNECTED",
            "message": "StreamForge CDC Mutation Stream Active"
        })
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "PONG"})
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
