use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Waker};

use crate::task::Task;

/// Creates a Waker from a Task.
/// When the waker is woken, it will re-enqueue the task to the executor.
pub fn create_waker(task: Arc<Task>) -> Waker {
    // We use Arc<Task> as the data pointer
    let raw_waker = RawWaker::new(Arc::into_raw(task) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}

/// The virtual function table for our waker implementation.
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

/// Clone the waker by incrementing the Arc refcount.
unsafe fn clone_waker(data: *const ()) -> RawWaker {
    let task = Arc::from_raw(data as *const Task);
    let cloned = Arc::clone(&task);
    // Don't drop the original Arc
    std::mem::forget(task);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

/// Wake by value - consumes the waker and re-enqueues the task.
unsafe fn wake(data: *const ()) {
    let task = Arc::from_raw(data as *const Task);
    // Re-enqueue the task
    crate::executor::enqueue(task);
}

/// Wake by reference - re-enqueues the task without consuming the waker.
unsafe fn wake_by_ref(data: *const ()) {
    let task = Arc::from_raw(data as *const Task);
    crate::executor::enqueue(Arc::clone(&task));
    // Don't drop the Arc
    std::mem::forget(task);
}

/// Drop the waker by decrementing the Arc refcount.
unsafe fn drop_waker(data: *const ()) {
    let _ = Arc::from_raw(data as *const Task);
    // Arc is dropped here
}
