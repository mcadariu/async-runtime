use std::time::{Duration, Instant};

fn main() {
    println!("=== Async Runtime Benchmark ===\n");

    benchmark_task_spawning();
    benchmark_sleep_performance();
}

fn benchmark_task_spawning() {
    println!("1. Task Spawning Throughput");
    println!("----------------------------");

    {
        let rt = async_runtime::Runtime::new();
        let start = Instant::now();
        let num_tasks = 10_000;

        rt.block_on(async move {
            for _ in 0..num_tasks {
                async_runtime::spawn(async {
                });
            }

            async_runtime::sleep(Duration::from_millis(100)).await;
        });

        let elapsed = start.elapsed();
        println!(
            "  async-runtime: Spawned {} tasks in {:?} ({:.0} tasks/sec)",
            num_tasks,
            elapsed,
            num_tasks as f64 / elapsed.as_secs_f64()
        );
    }

    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let start = Instant::now();
        let num_tasks = 10_000;

        rt.block_on(async {
            for _ in 0..num_tasks {
                tokio::spawn(async {
                });
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let elapsed = start.elapsed();
        println!(
            "  tokio:        Spawned {} tasks in {:?} ({:.0} tasks/sec)",
            num_tasks,
            elapsed,
            num_tasks as f64 / elapsed.as_secs_f64()
        );
    }

    println!();
}

fn benchmark_sleep_performance() {
    println!("2. Sleep/Timer Performance");
    println!("--------------------------");

    {
        let rt = async_runtime::Runtime::new();
        let start = Instant::now();

        rt.block_on(async {
            for _ in 0..10 {
                async_runtime::sleep(Duration::from_millis(10)).await;
            }
        });

        let elapsed = start.elapsed();
        println!("  async-runtime: 10x 10ms sleeps took {:?}", elapsed);
    }

    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let start = Instant::now();

        rt.block_on(async {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let elapsed = start.elapsed();
        println!("  tokio:        10x 10ms sleeps took {:?}", elapsed);
    }

    println!();
}
