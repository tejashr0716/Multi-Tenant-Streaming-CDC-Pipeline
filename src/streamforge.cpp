#include "streamforge.h"
#include <cstring>
#include <cstdio>
#include <algorithm>

// 64-bit FNV-1a Hash
static uint64_t fnv1a_64(const char* str, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (uint64_t)(unsigned char)str[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

extern "C" {

void route_cdc_mutation(const CDCMutation* mutation, int32_t total_partitions, RoutingDecision* out_decision) {
    if (!mutation || !out_decision) return;

    int32_t num_parts = (total_partitions > 0) ? total_partitions : 16;

    // Composite key: tenant_id + ":" + record_pk
    char composite[128];
    int len = std::snprintf(composite, sizeof(composite), "%s:%s", mutation->tenant_id, mutation->record_pk);

    uint64_t hash = fnv1a_64(composite, (len > 0) ? len : 0);
    int32_t part_id = (int32_t)(hash % (uint64_t)num_parts);

    out_decision->partition_id = part_id;
    out_decision->is_duplicate = 0;
    out_decision->record_hash = hash;

    std::snprintf(out_decision->routing_key, sizeof(out_decision->routing_key),
                  "streamforge.%s.part_%d", mutation->tenant_id, part_id);
}

void batch_route_mutations(const CDCMutation* mutations, int32_t count, int32_t total_partitions, RoutingDecision* out_decisions) {
    if (!mutations || !out_decisions || count <= 0) return;

    // Loop unrolled for high CPU pipeline efficiency
    int32_t i = 0;
    for (; i <= count - 4; i += 4) {
        route_cdc_mutation(&mutations[i], total_partitions, &out_decisions[i]);
        route_cdc_mutation(&mutations[i+1], total_partitions, &out_decisions[i+1]);
        route_cdc_mutation(&mutations[i+2], total_partitions, &out_decisions[i+2]);
        route_cdc_mutation(&mutations[i+3], total_partitions, &out_decisions[i+3]);
    }
    for (; i < count; ++i) {
        route_cdc_mutation(&mutations[i], total_partitions, &out_decisions[i]);
    }
}

int32_t check_lsn_monotonicity(int64_t last_seen_lsn, int64_t incoming_lsn) {
    // If incoming LSN is strictly greater, it is a valid forward mutation
    if (incoming_lsn > last_seen_lsn) {
        return 1; // Valid new mutation
    }
    return 0; // Stale or duplicate LSN replay
}

} // extern "C"
