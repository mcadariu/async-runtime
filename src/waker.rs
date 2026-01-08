use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Waker};

use crate::task::Task;

pub fn create_waker(task: Arc<Task>) -> Waker {
    let raw_waker = RawWaker::new(Arc::into_raw(task) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    let task = Arc::from_raw(data as *const Task);
    let cloned = Arc::clone(&task);
    std::mem::forget(task);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn wake(data: *const ()) {
    let task = Arc::from_raw(data as *const Task);
    crate::executor::enqueue(task);
}

unsafe fn wake_by_ref(data: *const ()) {
    let task = Arc::from_raw(data as *const Task);
    crate::executor::enqueue(Arc::clone(&task));
    std::mem::forget(task);
}

unsafe fn drop_waker(data: *const ()) {
    let _ = Arc::from_raw(data as *const Task);
}
