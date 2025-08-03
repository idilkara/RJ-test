## Repository Structure

- **`baselines/obliviatorFK-TDX/`** - Obliviator's foreign-key join ported to run outside SGX
- **`baselines/obliviatorNFK-TDX/`** - Obliviator's non-foreign key join ported to run outside SGX
- **`radixFK/`** - Our radix partitioning-based join for foreign key relationships
- **`radixNFK/`** - Our radix partitioning-based join for non-foreign key relationships


## Build Instructions

### Building Baseline Implementations

For both `obliviatorNFK-TDX` and `obliviatorFK-TDX`:

```bash
cd baselines/obliviatorNFK-TDX  # or baselines/obliviatorFK-TDX
make -f Makefile.standalone clean
make -f Makefile.standalone
```

This builds the `standalone_join` executable that can be run with the following command:
```bash
./standalone_join <num_threads> <input_file>
```

### Building Radix Partitioning-based Implementations

For both `radixNFK` and `radixFK`:

```bash
cd radixNFK  # or radixFK
mkdir build && cd build
cmake ..
make -j$(nproc)
```

This builds the `OblRadix` executable that can be run with the following command:
```bash
./OblRadix <num_threads> <input_file>
```

**Note**: The radix partitioning-based joins are hardware-conscious algorithms. Depending on your workload and hardware, you may need to adjust default configurations for optimal performance:

- **Radix parameters**: Modify `radixFK/external/radix_partition/CMakeLists.txt` (or `radixNFK/external/radix_partition/CMakeLists.txt`) to update:
  - `NUM_RADIX_BITS` (default: 10)  
  - `NUM_PASSES` (default: 2)

- **Cache parameters**: Modify `radixFK/external/radix_partition/prj_params.h` (or `radixNFK/external/radix_partition/prj_params.h`) to update:
  - `CACHE_LINE_SIZE` (default: 64)
  - `L1_CACHE_SIZE` (default: 49152) 
  - `L1_ASSOCIATIVITY` (default: 12)

## Testing and Validation

Our radix partitioning-based implementations include Python output validation scripts that compare the C++ implementation results against a reference pandas implementation to ensure correctness:

```bash
# Validate results using Python script
cd radixNFK  # or radixFK
# First build and run the program to generate output:
# ./OblRadix <num_threads> <input_file>
# Then validate the results:
python3 TestOutput.py <input_file> [join_output_file (build/join.txt by default)]
```
