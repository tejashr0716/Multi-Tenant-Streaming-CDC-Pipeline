#ifndef STREAMFORGE_H
#define STREAMFORGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(push, 8)
typedef struct {
    char tenant_id[32];
    char table_name[32];
    char record_pk[64];
    int64_t lsn_offset;
    int32_t op_type; // 0: INSERT, 1: UPDATE, 2: DELETE
    uint32_t payload_crc;
} CDCMutation;

typedef struct {
    int32_t partition_id;     // 0 to (total_partitions - 1)
    int32_t is_duplicate;     // 0 or 1
    uint64_t record_hash;     // 64-bit partition hash
    char routing_key[64];     // "streamforge.tenant.partition_X"
} RoutingDecision;
#pragma pack(pop)

// Route single CDC mutation to partition and calculate composite hash
void route_cdc_mutation(const CDCMutation* mutation, int32_t total_partitions, RoutingDecision* out_decision);

// Vectorized batch partition routing
void batch_route_mutations(const CDCMutation* mutations, int32_t count, int32_t total_partitions, RoutingDecision* out_decisions);

// Microsecond LSN monotonicity and replay check
int32_t check_lsn_monotonicity(int64_t last_seen_lsn, int64_t incoming_lsn);

#ifdef __cplusplus
}
#endif

#endif // STREAMFORGE_H
