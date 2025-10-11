

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

int main() {
    const uint64_t data_max = (1ULL << 31);   // 2^31
    const std::vector<uint64_t> ratios = {1000, 1000000}; //1000 and 1Million
    uint64_t n_big = 400000000; // 400 Million

            // --- Generate unique values for the big table ---
        std::mt19937_64 gen_big(42);
        std::uniform_int_distribution<uint64_t> dist_big(1, data_max - 1);

        std::unordered_set<uint64_t> unique_values;
        unique_values.reserve(n_big * 1.2);

        while (unique_values.size() < n_big) {
            unique_values.insert(dist_big(gen_big));
        }

        std::vector<uint64_t> values_big(unique_values.begin(), unique_values.end());

        
    for (auto ratio : ratios) {
        uint64_t n_small = ratio;

        // --- Generate small table sampled from big table ---
        std::mt19937_64 gen_small(123);
        std::uniform_int_distribution<uint64_t> dist_small(0, n_big - 1);

        std::vector<uint64_t> values_small(n_small);
        for (uint64_t i = 0; i < n_small; ++i) {
            values_small[i] = values_big[dist_small(gen_small)];
        }

        // --- Write to file ---
        std::string name = "./contacts_" + std::to_string(ratio) + "to400M.txt";
        std::ofstream file(name);
        if (!file) {
            std::cerr << "Error opening file " << name << "\n";
            continue;
        }

        file << n_big << " " << n_small << "\n\n";

        // Table S (unique)
        for (auto val : values_big)
            file << val << " " << val << "\n";

        file << "\n";

        // Table R (subset of S, with replacement)
        for (auto val : values_small)
            file << val << " " << val << "\n";

        file.close();

        std::cout << "Generated " << name << ": S=" << n_big
                  << " (unique), R=" << n_small << " (sampled from S)\n";
    }

    return 0;
}
