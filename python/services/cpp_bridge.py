import ctypes
import os
from typing import List, Dict, Any, Optional

class CCDCMutation(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("tenant_id", ctypes.c_char * 32),
        ("table_name", ctypes.c_char * 32),
        ("record_pk", ctypes.c_char * 64),
        ("lsn_offset", ctypes.c_int64),
        ("op_type", ctypes.c_int32),
        ("payload_crc", ctypes.c_uint32)
    ]

class CRoutingDecision(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("partition_id", ctypes.c_int32),
        ("is_duplicate", ctypes.c_int32),
        ("record_hash", ctypes.c_uint64),
        ("routing_key", ctypes.c_char * 64)
    ]

class StreamForgeEngine:
    def __init__(self, dylib_path: Optional[str] = None):
        if dylib_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            dylib_path = os.path.join(base_dir, "libstreamforge.dylib")

        if not os.path.exists(dylib_path):
            raise FileNotFoundError(f"Shared library not found at: {dylib_path}")

        self.lib = ctypes.CDLL(dylib_path)

        # void route_cdc_mutation(const CDCMutation* mutation, int32_t total_partitions, RoutingDecision* out_decision);
        self.lib.route_cdc_mutation.argtypes = [
            ctypes.POINTER(CCDCMutation),
            ctypes.c_int32,
            ctypes.POINTER(CRoutingDecision)
        ]
        self.lib.route_cdc_mutation.restype = None

        # void batch_route_mutations(...)
        self.lib.batch_route_mutations.argtypes = [
            ctypes.POINTER(CCDCMutation),
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.POINTER(CRoutingDecision)
        ]
        self.lib.batch_route_mutations.restype = None

        # int32_t check_lsn_monotonicity(int64_t last_seen_lsn, int64_t incoming_lsn);
        self.lib.check_lsn_monotonicity.argtypes = [ctypes.c_int64, ctypes.c_int64]
        self.lib.check_lsn_monotonicity.restype = ctypes.c_int32

    def route_mutation(self, tenant_id: str, table_name: str, record_pk: str, lsn: int, op_type: int = 0, total_partitions: int = 16) -> Dict[str, Any]:
        mut = CCDCMutation(
            tenant_id=tenant_id.encode("utf-8"),
            table_name=table_name.encode("utf-8"),
            record_pk=record_pk.encode("utf-8"),
            lsn_offset=lsn,
            op_type=op_type,
            payload_crc=0
        )
        dec = CRoutingDecision()
        self.lib.route_cdc_mutation(ctypes.byref(mut), total_partitions, ctypes.byref(dec))

        return {
            "partition_id": dec.partition_id,
            "record_hash": dec.record_hash,
            "routing_key": dec.routing_key.decode("utf-8")
        }

    def batch_route(self, items: List[Dict[str, Any]], total_partitions: int = 16) -> List[Dict[str, Any]]:
        n = len(items)
        if n == 0:
            return []

        mut_arr = (CCDCMutation * n)()
        dec_arr = (CRoutingDecision * n)()

        for i, item in enumerate(items):
            mut_arr[i] = CCDCMutation(
                tenant_id=str(item.get("tenant_id", "TENANT_DEFAULT")).encode("utf-8"),
                table_name=str(item.get("table_name", "records")).encode("utf-8"),
                record_pk=str(item.get("record_pk", "pk_0")).encode("utf-8"),
                lsn_offset=int(item.get("lsn_offset", 0)),
                op_type=int(item.get("op_type", 0)),
                payload_crc=0
            )

        self.lib.batch_route_mutations(mut_arr, n, total_partitions, dec_arr)

        output = []
        for i in range(n):
            output.append({
                "partition_id": dec_arr[i].partition_id,
                "record_hash": dec_arr[i].record_hash,
                "routing_key": dec_arr[i].routing_key.decode("utf-8")
            })
        return output

    def is_valid_lsn(self, last_lsn: int, incoming_lsn: int) -> bool:
        return bool(self.lib.check_lsn_monotonicity(ctypes.c_int64(last_lsn), ctypes.c_int64(incoming_lsn)))
