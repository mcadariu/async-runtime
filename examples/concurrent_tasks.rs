use async_runtime::{sleep, spawn, Runtime};
use std::time::Duration;

fn main() {
    println!("=== Concurrent Tasks Example ===\n");

    let rt = Runtime::new();

    rt.block_on(async {
        println!("Spawning 10 concurrent tasks...\n");

        // Spawn 10 tasks that run concurrently
        for i in 0..10 {
            spawn(async move {
                println!("Task {} starting", i);
                sleep(Duration::from_millis(100 * (i as u64 + 1))).await;
                println!("Task {} completed after {}ms", i, 100 * (i + 1));
            });
        }

        // Give tasks time to complete
        sleep(Duration::from_secs(2)).await;

        println!("\nAll tasks completed!");
    });
}
