# Experiments Guide

This document provides hands-on experiments you can run to understand how the async runtime works.

## Quick Start

### 1. Run All Examples

```bash
# Concurrent tasks
cargo run --example concurrent_tasks

# Echo server (needs separate terminal)
cargo run --example echo_server
# Test with: echo "Hello" | nc localhost 8080

# HTTP server (needs separate terminal)
cargo run --example http_server
# Test with: curl http://localhost:8080

# Benchmark vs Tokio
cargo run --example benchmark --release
```

## Detailed Experiments

### Experiment 1: Understanding Concurrent Execution

**Goal**: Prove that tasks run truly concurrently, not sequentially.

```bash
# Run the concurrent tasks example
cargo run --example concurrent_tasks
```

**Observe**:
- Tasks start in a non-sequential order (due to thread pool)
- All tasks complete in ~1.1 seconds total (not 5.5 seconds if sequential)
- This proves the multi-threaded executor is working

**Try this modification**: Edit `examples/concurrent_tasks.rs` and change the number of tasks from 10 to 100. What happens to the startup pattern?

### Experiment 2: Thread Pool Impact

**Goal**: See how the number of worker threads affects throughput.

Edit `src/executor.rs:130` to test different configurations:

```rust
// Option 1: Single-threaded
Runtime::with_threads(1)

// Option 2: Two threads
Runtime::with_threads(2)

// Option 3: Many threads
Runtime::with_threads(16)
```

Then run:
```bash
cargo run --example benchmark --release
```

**Expected results**:
- 1 thread: Lower throughput
- 2-4 threads: Good balance
- 16 threads: Diminishing returns (overhead from synchronization)

### Experiment 3: I/O Performance Test

**Goal**: Measure concurrent connection handling.

Terminal 1:
```bash
cargo run --example echo_server --release
```

Terminal 2:
```bash
# Single connection
time (echo "test" | nc localhost 8080)

# 10 sequential connections
for i in {1..10}; do
    echo "Request $i" | nc localhost 8080
done

# 10 concurrent connections (requires GNU parallel or xargs)
seq 1 10 | xargs -P 10 -I {} sh -c 'echo "Request {}" | nc localhost 8080'
```

**Observe**:
- Sequential: 10x the single connection time
- Concurrent: Only slightly more than single connection time
- This proves the reactor is multiplexing I/O efficiently

### Experiment 4: Reactor Polling Interval

**Goal**: Understand the trade-off between latency and CPU usage.

Edit `src/reactor.rs:100` and try different timeouts:

```rust
// Low latency, high CPU
poll.poll(&mut events, Some(Duration::from_millis(1)))

// Balanced (default)
poll.poll(&mut events, Some(Duration::from_millis(100)))

// High latency, low CPU
poll.poll(&mut events, Some(Duration::from_millis(1000)))
```

Then test echo server latency:
```bash
# Terminal 1
cargo run --example echo_server --release

# Terminal 2
time (echo "test" | nc localhost 8080)
```

**Expected results**:
- 1ms timeout: ~1-2ms latency, high CPU usage (check with `top`)
- 100ms timeout: ~100-110ms latency, moderate CPU
- 1000ms timeout: ~1000-1010ms latency, very low CPU

### Experiment 5: Task Spawning Overhead

**Goal**: Measure the cost of spawning async tasks.

Edit `examples/benchmark.rs` and change `num_tasks`:

```rust
let num_tasks = 1_000;    // Baseline
let num_tasks = 10_000;   // Default
let num_tasks = 100_000;  // Stress test
```

Run:
```bash
cargo run --example benchmark --release
```

**Analysis**:
- Calculate tasks per millisecond
- Compare with Tokio's performance
- Our runtime should be 60-80% as fast (which is great for educational code!)

### Experiment 6: Waker Overhead Measurement

**Goal**: Count how many times wakers are created and called.

Add instrumentation to `src/waker.rs`:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

static WAKER_CREATES: AtomicUsize = AtomicUsize::new(0);
static WAKER_WAKES: AtomicUsize = AtomicUsize::new(0);

pub fn create_waker(task: Arc<Task>) -> Waker {
    WAKER_CREATES.fetch_add(1, Ordering::Relaxed);
    // ... rest of function
}

unsafe fn wake(data: *const ()) {
    WAKER_WAKES.fetch_add(1, Ordering::Relaxed);
    // ... rest of function
}

// Add getter functions
pub fn stats() -> (usize, usize) {
    (WAKER_CREATES.load(Ordering::Relaxed), 
     WAKER_WAKES.load(Ordering::Relaxed))
}
```

Then print stats in your examples.

### Experiment 7: Memory Usage

**Goal**: See how much memory the runtime uses.

```bash
# Terminal 1: Run with many tasks
cargo run --example concurrent_tasks --release

# Terminal 2: Monitor memory
# macOS:
ps aux | grep concurrent_tasks

# Or use Activity Monitor
```

**Try this**: Modify concurrent_tasks to spawn 10,000 tasks instead of 10. How much does memory usage increase?

### Experiment 8: Stress Testing

**Goal**: Find the breaking point.

For HTTP server:
```bash
# Terminal 1
cargo run --example http_server --release

# Terminal 2 (if you have Apache Bench installed)
ab -n 10000 -c 100 http://localhost:8080/

# Or with curl in a loop
for i in {1..1000}; do
    curl http://localhost:8080/ &
done
```

**Observe**:
- How many requests/sec can it handle?
- Does it crash or slow down gracefully?
- How does it compare to a Tokio-based server?

### Experiment 9: Timer Accuracy

**Goal**: Measure how accurate our sleep implementation is.

Create `examples/timer_test.rs`:

```rust
use async_runtime::{Runtime, sleep};
use std::time::{Duration, Instant};

fn main() {
    let rt = Runtime::new();
    
    for duration_ms in [1, 10, 100, 1000] {
        let start = Instant::now();
        rt.block_on(async move {
            sleep(Duration::from_millis(duration_ms)).await;
        });
        let actual = start.elapsed().as_millis();
        let expected = duration_ms as u128;
        let error = if actual > expected { 
            actual - expected 
        } else { 
            expected - actual 
        };
        println!("Expected: {}ms, Actual: {}ms, Error: {}ms", 
                 expected, actual, error);
    }
}
```

**Expected results**: Error increases with longer sleeps due to thread spawning overhead.

### Experiment 10: CPU-Bound vs I/O-Bound

**Goal**: Understand when async shines and when it struggles.

Create `examples/cpu_vs_io.rs`:

```rust
use async_runtime::{Runtime, sleep, spawn};
use std::time::{Duration, Instant};

fn main() {
    let rt = Runtime::new();
    
    println!("=== I/O-Bound Tasks (Good for async) ===");
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..10 {
            spawn(async {
                sleep(Duration::from_millis(100)).await;
            });
        }
        sleep(Duration::from_millis(200)).await;
    });
    println!("10 I/O tasks took: {:?}", start.elapsed());
    
    println!("\n=== CPU-Bound Tasks (Bad for async) ===");
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..10 {
            spawn(async {
                // CPU-intensive work blocks the executor!
                let mut sum = 0u64;
                for i in 0..100_000_000 {
                    sum = sum.wrapping_add(i);
                }
            });
        }
        sleep(Duration::from_secs(2)).await;
    });
    println!("10 CPU tasks took: {:?}", start.elapsed());
}
```

**Key insight**: CPU-bound work blocks executor threads, making async less effective than I/O-bound work.

## Performance Tuning

### Optimize for Throughput
- Increase worker threads (up to CPU count)
- Decrease reactor poll timeout
- Use release builds (`--release`)

### Optimize for Latency
- Use smaller task queue sizes
- Decrease reactor poll timeout to 1-10ms
- Pin threads to CPU cores (advanced)

### Optimize for Memory
- Reduce number of concurrent tasks
- Use smaller buffer sizes in network code
- Consider work stealing to balance load

## Comparison with Production Runtimes

Run equivalent code with Tokio and compare:

```bash
# Our runtime
cargo run --example benchmark --release

# Tokio equivalent
# (Create similar example using Tokio directly)
```

**Why is Tokio faster?**
- Lock-free data structures
- Specialized allocators
- Years of optimization
- Assembly-level tuning

**But you learned how it works!** 🎓

## Further Exploration

1. **Add metrics**: Instrument the code to track queue depths, poll counts, etc.
2. **Visualize**: Create a web dashboard showing runtime statistics
3. **Profile**: Use `cargo flamegraph` to see where time is spent
4. **Compare**: Benchmark against async-std, smol, and other runtimes
5. **Extend**: Add UDP support, file I/O, or other features

## Debugging Tips

```bash
# Enable backtraces
RUST_BACKTRACE=1 cargo run --example concurrent_tasks

# Enable logging (if you add tracing)
RUST_LOG=debug cargo run --example echo_server

# Check for undefined behavior
cargo miri test  # (requires miri)

# Profile with instruments (macOS)
cargo build --example benchmark --release
instruments -t "Time Profiler" ./target/release/examples/benchmark
```

## Questions to Answer

1. What happens if you spawn 1,000,000 tasks?
2. How does performance change with 1 vs 4 vs 16 worker threads?
3. Can you saturate a gigabit network connection?
4. What's the minimum latency you can achieve?
5. How much memory does each task consume?

Happy experimenting!
