use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Sleep for a specified duration.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + duration,
    }
}

/// A future that completes after a specified time.
pub struct Sleep {
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            // Wake the task after the remaining time
            let waker = cx.waker().clone();
            let remaining = self.deadline - Instant::now();

            std::thread::spawn(move || {
                std::thread::sleep(remaining);
                waker.wake();
            });

            Poll::Pending
        }
    }
}
