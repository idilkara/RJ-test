#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>

#include "backfill_dummies.h"
#include "carry_forward.h"
#include "generate_hash_R.h"
#include "inputs.h"
#include "parallel_counts.h"
#include "prefix_sum_expand.h"
#include "replace_dummies.h"
#include "result_indices.h"
#include "slice_utils.h"

extern "C" {
#include "bitonic.h"
#include "radix_join_counts.h"
#include "radix_join_idx.h"
#include "threading.h"
}

// #define PRE_SORTED // use this if your tables are already sorted

// Global timer
std::chrono::high_resolution_clock::time_point tStart;

int main(int argc, char *argv[]) {
  printf("Set number of radix bits and passes for your workload in "
         "external/radix_partition/CMakeLists.txt.\n");
  std::uint32_t numThreads = 32;
  std::string inputPath = "../amazon.txt";

  if (argc > 1)
    numThreads = std::max<std::uint32_t>(1, std::stoul(argv[1]));
  if (argc > 2)
    inputPath = argv[2];
  if (argc > 3) {
    std::cerr << "Program takes 2 arguments: number of threads and input "
                 "filepath."
              << std::endl;
    return 1;
  }
  printf("Input   : %s\n", inputPath.c_str());
  printf("Threads : %u\n", numThreads);

  std::vector<Record> t0, t1;
  if (!load_two_tables(inputPath, t0, t1))
    return 1;

  table_t R, S;
  R.tuples = new row_t[t0.size()];
  std::memcpy(R.tuples, t0.data(), t0.size() * sizeof(Record));
  R.num_tuples = static_cast<uint32_t>(t0.size());
  for (int i = 0; i < R.num_tuples; i++) {
    R.tuples[i].cntSelf = 1;
  }

  S.tuples = new row_t[t1.size()];
  std::memcpy(S.tuples, t1.data(), t1.size() * sizeof(Record));
  S.num_tuples = static_cast<uint32_t>(t1.size());

  t0.clear();
  t0.shrink_to_fit();
  t1.clear();
  t1.shrink_to_fit();

  auto slices_S_numThreads = buildSlices(S.num_tuples, numThreads);
  auto slices_R_numThreads = buildSlices(R.num_tuples, numThreads);

  std::vector<int> lastLen(slices_S_numThreads.size()),
      mergeVal(slices_S_numThreads.size() - 1);

#ifndef PRE_SORTED
  extern size_t total_num_threads;
  total_num_threads = numThreads;
  thread_system_init();

  std::vector<std::thread> pool;
  for (size_t i = 1; i < numThreads; ++i)
    pool.emplace_back(thread_start_work);

  tStart = std::chrono::high_resolution_clock::now();
  bitonic_sort_(S.tuples, true, 0, S.num_tuples, numThreads, false);

  thread_release_all();
  for (auto &t : pool)
    t.join();
  thread_system_cleanup();
#else
  tStart = std::chrono::high_resolution_clock::now();
#endif

  parallelCounts(S, slices_S_numThreads, lastLen, mergeVal);
  generateHashParallel(R, slices_R_numThreads);
  replaceWithDummiesParallel(S, slices_S_numThreads);

  if (S.num_tuples >= R.num_tuples) {
    RHO(&R, &S, numThreads, false);
  } else {
    RHO(&S, &R, numThreads, true);
  }

  backfillDummiesParallel(S, slices_S_numThreads);
  std::uint32_t m = prefixSumExpandParallel(S, slices_S_numThreads);
  std::vector<Slice> slices_m = buildSlices(m, numThreads);
  const std::size_t bytes = m * sizeof(row_t);
  table_t idxTable{};
  idxTable.tuples = static_cast<row_t *>(aligned_alloc(32, bytes));
  idxTable.num_tuples = m;
  buildResultIndices(slices_m, idxTable);

  table_t expanded{};
  expanded.tuples = static_cast<row_t *>(aligned_alloc(32, bytes));
  std::memset(expanded.tuples, 0, bytes);
  expanded.num_tuples = m;

  if (m >= S.num_tuples) {
    RHO_idx(&S, &idxTable, numThreads, &expanded, true);
  } else {
    RHO_idx(&idxTable, &S, numThreads, &expanded, false);
  }

  carryForwardParallel(expanded, slices_m);

  {
    std::ofstream outER("join.txt");
    for (int i = 0; i < expanded.num_tuples; i++) {
      outER << expanded.tuples[i].key << ' ' << expanded.tuples[i].payPrimary
            << ' ' << expanded.tuples[i].key << ' '
            << expanded.tuples[i].paySelf << '\n';
    }
  }
  printf("Join result rows : %ld (written to join.txt)\n", expanded.num_tuples);

  return 0;
}
