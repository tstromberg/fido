# bdcache Benchmarks

This directory contains comparison benchmarks against popular Go cache libraries.

## ⚠️ Important Disclaimers

### Cherrypicked Benchmarks

These benchmarks are **intentionally cherrypicked** to demonstrate S3-FIFO's strengths:

- **Scan resistance workloads** - Where large scans of cold data shouldn't evict hot working set
- **One-hit wonder scenarios** - Where many items are accessed once and shouldn't pollute the cache
- **Memory-only Get operations** - Pure speed comparisons without I/O

**Different workloads favor different algorithms:**
- **LRU** excels with temporal locality and simple sequential access patterns
- **TinyLFU (Ristretto)** shines with frequency-based workloads and large caches
- **S3-FIFO** handles mixed workloads with both hot items and one-hit wonders

Your mileage **will** vary based on:
- Access patterns (sequential, random, zipfian, etc.)
- Working set size vs cache capacity
- Read/write ratio
- Key/value sizes
- Hardware (CPU, memory speed)

### The Real Differentiator: Persistence

**bdcache's primary advantage isn't raw speed or hit rates** - it's the automatic per-item persistence designed for unreliable cloud environments:

- **Cloud Run** - Instances shut down unpredictably after idle periods
- **Kubernetes** - Pods can be evicted, rescheduled, or killed anytime
- **Container environments** - Restarts lose all in-memory data
- **Crash recovery** - Application failures don't lose cache state

Other libraries require manual save/load of the entire cache, which:
- Doesn't work when shutdowns are unexpected
- Requires coordination and timing logic
- Risks data loss on crashes
- Adds operational complexity

## Running Benchmarks

### Speed Comparison

```bash
go test -bench=BenchmarkSpeed -benchmem
```

Compares raw Get operation performance across:
- bdcache (S3-FIFO)
- golang-lru (LRU)
- otter (S3-FIFO with manual persistence)
- ristretto (TinyLFU)

### Full Benchmark Suite

```bash
go test -run=TestBenchmarkSuite -v
```

Runs the complete benchmark comparison including hit rates, latency, and concurrent throughput across all thread counts (1, 4, 8, 12, 16, 24, 32).

## Benchmark Files

- `benchmark_test.go` - Speed, hit rate, and throughput benchmarks across libraries

## Interpreting Results

When evaluating caches for your use case:

1. **Profile your actual workload** - Synthetic benchmarks don't capture real-world complexity
2. **Measure what matters** - Hit rate, latency, throughput, memory usage
3. **Consider operational needs** - Persistence, observability, graceful degradation
4. **Test with your data** - Key/value sizes and access patterns vary wildly
5. **Benchmark in production-like environments** - Hardware and load matter

**Don't choose a cache based solely on these benchmarks.** Choose based on your specific requirements, with special attention to operational characteristics like persistence if you're running in unreliable cloud environments.
