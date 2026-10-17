#include "streamforge.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <iomanip>
#include <cstring>

int main() {
    std::cout << "========================================================\n";
    std::cout << "   StreamForge C++ CDC Partitioning & Hash Benchmark    \n";
    std::cout << "========================================================\n\n";

    const int N = 3000000;
    std::vector<CDCMutation> mutations(N);
    std::vector<RoutingDecision> decisions(N);

    for (int i = 0; i < N; ++i) {
        std::snprintf(mutations[i].tenant_id, sizeof(mutations[i].tenant_id), "TENANT_STRIPE_%02d", i % 5);
        std::snprintf(mutations[i].table_name, sizeof(mutations[i].table_name), "charges");
        std::snprintf(mutations[i].record_pk, sizeof(mutations[i].record_pk), "ch_1049284729_%d", i);
        mutations[i].lsn_offset = 1000000 + i;
        mutations[i].op_type = i % 3;
        mutations[i].payload_crc = 0x12345678;
    }

    // 1. Single Mutation Routing Throughput
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; ++i) {
        route_cdc_mutation(&mutations[i], 16, &decisions[i]);
    }
    auto end = std::chrono::high_resolution_clock::now();
    double single_sec = std::chrono::duration<double>(end - start).count();
    double single_ops = (double)N / single_sec;
    double single_ns = (single_sec / (double)N) * 1e9;

    std::cout << ">> Single CDC Hash Routing:       " << std::fixed << std::setprecision(2)
              << (single_ops / 1e6) << " Million mutations/sec ("
              << single_ns << " ns/op)\n";

    // 2. Vectorized Batch Mutation Routing
    start = std::chrono::high_resolution_clock::now();
    batch_route_mutations(mutations.data(), N, 16, decisions.data());
    end = std::chrono::high_resolution_clock::now();
    double batch_sec = std::chrono::duration<double>(end - start).count();
    double batch_ops = (double)N / batch_sec;
    double batch_ns = (batch_sec / (double)N) * 1e9;

    std::cout << ">> Batch Vectorized Routing:      " << std::fixed << std::setprecision(2)
              << (batch_ops / 1e6) << " Million mutations/sec ("
              << batch_ns << " ns/op)\n";

    // 3. LSN Monotonicity Check
    const int LSN_RUNS = 10000000;
    int32_t valid_count = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < LSN_RUNS; ++i) {
        valid_count += check_lsn_monotonicity(100, 105);
    }
    end = std::chrono::high_resolution_clock::now();
    double lsn_sec = std::chrono::duration<double>(end - start).count();
    double lsn_ops = (double)LSN_RUNS / lsn_sec;
    double lsn_ns = (lsn_sec / (double)LSN_RUNS) * 1e9;

    std::cout << ">> LSN Deduplication Check:       " << std::fixed << std::setprecision(2)
              << (lsn_ops / 1e6) << " Million checks/sec ("
              << lsn_ns << " ns/op)\n";

    std::cout << "\nSample Routing Output:\n";
    std::cout << "  Record:      " << mutations[0].record_pk << "\n";
    std::cout << "  Partition:   " << decisions[0].partition_id << " (of 16)\n";
    std::cout << "  Routing Key: " << decisions[0].routing_key << "\n";
    std::cout << "========================================================\n";

    return 0;
}
