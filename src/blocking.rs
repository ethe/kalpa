use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar, Mutex, MutexGuard,
    },
    thread,
    time::Duration,
};

use async_task::Runnable;
pub use async_task::Task;
use futures_lite::prelude::*;

use crate::BLOCKING_EXECUTOR;

/// The blocking executor.
pub(crate) struct Executor {
    /// Inner state of the executor.
    inner: Mutex<Inner>,

    /// Used to put idle threads to sleep and wake them up when new work comes in.
    cvar: Condvar,

    /// Maximum number of threads in the pool
    thread_limit: usize,
}

/// Inner state of the blocking executor.
struct Inner {
    /// Number of idle threads in the pool.
    ///
    /// Idle threads are sleeping, waiting to get a task to run.
    idle_count: usize,

    /// Total number of threads in the pool.
    ///
    /// This is the number of idle threads + the number of active threads.
    thread_count: usize,

    /// The queue of blocking tasks.
    queue: VecDeque<Runnable>,
}

impl Executor {
    pub(crate) fn new(thread_limit: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                idle_count: 0,
                thread_count: 0,
                queue: VecDeque::new(),
            }),
            cvar: Condvar::new(),
            thread_limit,
        }
    }

    /// Spawns a future onto this executor.
    ///
    /// Returns a [`Task`] handle for the spawned task.
    pub(crate) fn spawn<T: Send + 'static>(
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T> {
        let (runnable, task) = async_task::spawn(future, |r| {
            // Initialize the executor if we haven't already.
            let executor = BLOCKING_EXECUTOR
                .get()
                .expect("executor must be initialized");

            // Schedule the task on our executor.
            executor.schedule(r)
        });
        runnable.schedule();
        task
    }

    /// Runs the main loop on the current thread.
    ///
    /// This function runs blocking tasks until it becomes idle and times out.
    fn main_loop(&'static self) {
        let mut inner = self.inner.lock().unwrap();
        loop {
            // This thread is not idle anymore because it's going to run tasks.
            inner.idle_count -= 1;

            // Run tasks in the queue.
            while let Some(runnable) = inner.queue.pop_front() {
                // We have found a task - grow the pool if needed.
                self.grow_pool(inner);

                // Run the task.
                runnable.run();

                // Re-lock the inner state and continue.
                inner = self.inner.lock().unwrap();
            }

            // This thread is now becoming idle.
            inner.idle_count += 1;

            // Put the thread to sleep until another task is scheduled.
            let timeout = Duration::from_millis(500);
            let (lock, res) = self.cvar.wait_timeout(inner, timeout).unwrap();
            inner = lock;

            // If there are no tasks after a while, stop this thread.
            if res.timed_out() && inner.queue.is_empty() {
                inner.idle_count -= 1;
                inner.thread_count -= 1;
                break;
            }
        }
    }

    /// Schedules a runnable task for execution.
    fn schedule(&'static self, runnable: Runnable) {
        let mut inner = self.inner.lock().unwrap();
        inner.queue.push_back(runnable);

        // Notify a sleeping thread and spawn more threads if needed.
        self.cvar.notify_one();
        self.grow_pool(inner);
    }

    /// Spawns more blocking threads if the pool is overloaded with work.
    fn grow_pool(&'static self, mut inner: MutexGuard<'static, Inner>) {
        // If runnable tasks greatly outnumber idle threads and there aren't too many threads
        // already, then be aggressive: wake all idle threads and spawn one more thread.
        while inner.queue.len() > inner.idle_count * 5 && inner.thread_count < self.thread_limit {
            // The new thread starts in idle state.
            inner.idle_count += 1;
            inner.thread_count += 1;

            // Notify all existing idle threads because we need to hurry up.
            self.cvar.notify_all();

            // Generate a new thread ID.
            static ID: AtomicUsize = AtomicUsize::new(1);
            let id = ID.fetch_add(1, Ordering::Relaxed);

            // Spawn the new thread.
            thread::Builder::new()
                .name(format!("blocking-{}", id))
                .spawn(move || self.main_loop())
                .unwrap();
        }
    }
}
