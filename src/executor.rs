use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::Context;
use std::thread;

use crate::reactor::Reactor;
use crate::task::Task;
use crate::waker::create_waker;

struct ExecutorPtr(*const Executor);
unsafe impl Send for ExecutorPtr {}
unsafe impl Sync for ExecutorPtr {}

static EXECUTOR: OnceLock<ExecutorPtr> = OnceLock::new();

pub struct Executor {
    queue: Mutex<VecDeque<Arc<Task>>>,
    condvar: Condvar,
    reactor: Arc<Reactor>,
}

impl Executor {
    pub fn new(num_threads: usize) -> Arc<Self> {
        let executor = Arc::new(Executor {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            reactor: Arc::new(Reactor::new().expect("Failed to create reactor")),
        });

        for id in 0..num_threads {
            let exec = Arc::clone(&executor);
            thread::spawn(move || {
                exec.worker_loop(id);
            });
        }

        let reactor = Arc::clone(&executor.reactor);
        thread::spawn(move || {
            reactor.run();
        });

        executor
    }

    fn worker_loop(&self, _id: usize) {
        loop {
            let task = {
                let mut queue = self.queue.lock().unwrap();
                while queue.is_empty() {
                    queue = self.condvar.wait(queue).unwrap();
                }
                queue.pop_front()
            };

            if let Some(task) = task {
                let waker = create_waker(Arc::clone(&task));
                let mut context = Context::from_waker(&waker);

                if task.poll(&mut context).is_pending() {
                }
            }
        }
    }

    pub fn enqueue_task(&self, task: Arc<Task>) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(task);
        self.condvar.notify_one();
    }

    pub fn reactor(&self) -> &Arc<Reactor> {
        &self.reactor
    }
}

fn init_executor() -> &'static Executor {
    let exec_ptr = EXECUTOR.get_or_init(|| {
        let num_threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let executor = Executor::new(num_threads);
        ExecutorPtr(Arc::into_raw(executor))
    });
    unsafe { &*exec_ptr.0 }
}

pub fn enqueue(task: Arc<Task>) {
    let executor = init_executor();
    executor.enqueue_task(task);
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let task = Task::new(future);
    enqueue(task);
}

pub fn reactor() -> Arc<Reactor> {
    Arc::clone(init_executor().reactor())
}

pub struct Runtime {
    executor: Arc<Executor>,
}

impl Runtime {
    pub fn new() -> Self {
        let num_threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Runtime {
            executor: Executor::new(num_threads),
        }
    }

    pub fn with_threads(num_threads: usize) -> Self {
        Runtime {
            executor: Executor::new(num_threads),
        }
    }

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

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Task::new(future);
        self.executor.enqueue_task(task);
    }

    pub fn reactor(&self) -> Arc<Reactor> {
        Arc::clone(self.executor.reactor())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
