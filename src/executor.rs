use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::Context;
use std::thread;

use crate::reactor::Reactor;
use crate::task::Task;
use crate::waker::create_waker;

/// Wrapper to make the raw pointer Send + Sync (safe because it points to leaked Arc).
struct ExecutorPtr(*const Executor);
unsafe impl Send for ExecutorPtr {}
unsafe impl Sync for ExecutorPtr {}

/// Global executor instance (stored as a raw pointer to leak the Arc).
static EXECUTOR: OnceLock<ExecutorPtr> = OnceLock::new();

/// The executor manages a queue of tasks and polls them.
pub struct Executor {
    /// Queue of tasks ready to be polled.
    queue: Mutex<VecDeque<Arc<Task>>>,
    /// Condvar to wake worker threads when tasks are available.
    condvar: Condvar,
    /// I/O reactor for handling async I/O events.
    reactor: Arc<Reactor>,
}

impl Executor {
    /// Create a new executor with the specified number of worker threads.
    pub fn new(num_threads: usize) -> Arc<Self> {
        let executor = Arc::new(Executor {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            reactor: Arc::new(Reactor::new().expect("Failed to create reactor")),
        });

        // Spawn worker threads
        for id in 0..num_threads {
            let exec = Arc::clone(&executor);
            thread::spawn(move || {
                exec.worker_loop(id);
            });
        }

        // Start reactor thread
        let reactor = Arc::clone(&executor.reactor);
        thread::spawn(move || {
            reactor.run();
        });

        executor
    }

    /// Worker thread loop - continuously polls tasks from the queue.
    fn worker_loop(&self, _id: usize) {
        loop {
            // Get next task from queue
            let task = {
                let mut queue = self.queue.lock().unwrap();
                while queue.is_empty() {
                    queue = self.condvar.wait(queue).unwrap();
                }
                queue.pop_front()
            };

            if let Some(task) = task {
                // Create waker for this task
                let waker = create_waker(Arc::clone(&task));
                let mut context = Context::from_waker(&waker);

                // Poll the task
                if task.poll(&mut context).is_pending() {
                    // Task is still pending, it will be re-enqueued when woken
                }
                // If ready, task is complete and dropped
            }
        }
    }

    /// Enqueue a task to be executed.
    pub fn enqueue_task(&self, task: Arc<Task>) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(task);
        // Notify one worker thread
        self.condvar.notify_one();
    }

    /// Get a reference to the reactor.
    pub fn reactor(&self) -> &Arc<Reactor> {
        &self.reactor
    }
}

/// Initialize the global executor with the default number of threads.
fn init_executor() -> &'static Executor {
    let exec_ptr = EXECUTOR.get_or_init(|| {
        let num_threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let executor = Executor::new(num_threads);
        // Leak the Arc to get a static reference
        // This is fine since the global executor lives for the entire program
        ExecutorPtr(Arc::into_raw(executor))
    });
    unsafe { &*exec_ptr.0 }
}

/// Enqueue a task to the global executor.
pub fn enqueue(task: Arc<Task>) {
    let executor = init_executor();
    executor.enqueue_task(task);
}

/// Spawn a future on the global executor.
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let task = Task::new(future);
    enqueue(task);
}

/// Get the global reactor.
pub fn reactor() -> Arc<Reactor> {
    Arc::clone(init_executor().reactor())
}

/// The Runtime provides a convenient interface to run async code.
pub struct Runtime {
    executor: Arc<Executor>,
}

impl Runtime {
    /// Create a new runtime with default settings.
    pub fn new() -> Self {
        let num_threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Runtime {
            executor: Executor::new(num_threads),
        }
    }

    /// Create a new runtime with a specific number of worker threads.
    pub fn with_threads(num_threads: usize) -> Self {
        Runtime {
            executor: Executor::new(num_threads),
        }
    }

    /// Block the current thread until the future completes.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();

        let task_future = async move {
            let result = future.await;
            let _ = tx.send(result);
        };

        let task = Task::new(task_future);
        self.executor.enqueue_task(task);

        rx.recv().expect("Task panicked")
    }

    /// Spawn a task on this runtime.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Task::new(future);
        self.executor.enqueue_task(task);
    }

    /// Get a reference to the reactor.
    pub fn reactor(&self) -> Arc<Reactor> {
        Arc::clone(self.executor.reactor())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
