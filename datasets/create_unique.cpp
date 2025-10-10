#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

int main() {
    const uint64_t data_max = (1ULL << 31);   // 2^32
    const uint64_t total_size = (1ULL << 27); // 2^25
    const std::vector<uint64_t> ratios = {1, 10, 10000, 40000};

    for (auto ratio : ratios) {
        uint64_t n_big = (ratio * total_size) / (ratio + 1);
        uint64_t n_small = total_size - n_big;

        std::cout << "Generating ratio " << ratio << " (S=" << n_big
                  << ", R=" << n_small << ") ..." << std::endl;

        // === Generate S (big table) - unique random values ===
        std::mt19937_64 gen_big(42);
        std::uniform_int_distribution<uint64_t> dist_big(1, data_max - 1);
        std::unordered_set<uint64_t> set_big;
        set_big.reserve(n_big * 1.2);

        while (set_big.size() < n_big) {
            set_big.insert(dist_big(gen_big));
        }
        std::vector<uint64_t> values_big(set_big.begin(), set_big.end());

        // === Generate R (small table) - unique random values ===
        std::mt19937_64 gen_small(123);
        std::uniform_int_distribution<uint64_t> dist_small(1, data_max - 1);
        std::unordered_set<uint64_t> set_small;
        set_small.reserve(n_small * 1.2);

        while (set_small.size() < n_small) {
            set_small.insert(dist_small(gen_small));
        }
        std::vector<uint64_t> values_small(set_small.begin(), set_small.end());

        // === Write to file ===
        std::string name =
            "./unique_ratio_" + std::to_string(ratio) + "to1.txt";
        std::ofstream file(name);
        if (!file) {
            std::cerr << "Error opening file " << name << "\n";
            continue;
        }

        file << n_big << " " << n_small << "\n\n";

        // Table S (big one)
        for (auto val : values_big)
            file << val << " " << val << "\n";

        file << "\n";

        // Table R (small one)
        for (auto val : values_small)
            file << val << " " << val << "\n";

        file.close();

        std::cout << "Generated " << name << std::endl;
    }

    return 0;
}
