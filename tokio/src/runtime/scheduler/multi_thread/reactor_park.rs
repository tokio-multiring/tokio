//! Worker parker for a multi-threaded runtime driven by an external [`Reactor`].

use crate::runtime::driver;
use crate::runtime::io::reactor::{self, ParkMode, Reactor};
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::sync::Arc;
use std::time::Duration;

/// Per-worker parker: the worker's index and the runtime's reactor.
pub(crate) struct ReactorParker {
    worker: usize,
    reactor: Arc<dyn Reactor>,
}

/// Unparker counterpart to [`ReactorParker`].
#[derive(Clone)]
pub(crate) struct ReactorUnparker {
    worker: usize,
    reactor: Arc<dyn Reactor>,
}

impl ReactorParker {
    pub(crate) fn new(worker: usize, reactor: Arc<dyn Reactor>) -> Self {
        Self { worker, reactor }
    }

    pub(crate) fn unparker(&self) -> ReactorUnparker {
        ReactorUnparker {
            worker: self.worker,
            reactor: Arc::clone(&self.reactor),
        }
    }

    pub(crate) fn park(&mut self, driver: &driver::Handle) -> HadDriver {
        self.park_mode(driver, ParkMode::Park(None))
    }

    pub(crate) fn park_timeout(
        &mut self,
        driver: &driver::Handle,
        duration: Duration,
    ) -> HadDriver {
        let mode = if duration.is_zero() {
            ParkMode::Poll
        } else {
            ParkMode::Park(Some(duration))
        };
        self.park_mode(driver, mode)
    }

    fn park_mode(&mut self, driver: &driver::Handle, mode: ParkMode) -> HadDriver {
        // Report `HadDriver` faithfully so `enable_eager_driver_handoff` does
        // not spuriously wake siblings for workers that only waited.
        if reactor::park(&*self.reactor, driver, self.worker, mode) {
            HadDriver::Yes
        } else {
            HadDriver::No
        }
    }

    pub(crate) fn shutdown(&mut self, driver: &driver::Handle) {
        // The traditional `time::Driver` is not parked on a multi-threaded
        // reactor runtime, so shut down the time driver handle here too.
        #[cfg(feature = "time")]
        if let Some(time) = driver.time_handle_opt() {
            time.shutdown();
        }
        #[cfg(not(feature = "time"))]
        let _ = driver;

        self.reactor.shutdown();
    }
}

impl ReactorUnparker {
    /// Unparks the worker, even if the caller is the worker itself (the
    /// scheduler's `transition_to_parked` can pop the calling worker off the
    /// sleepers list and unpark it before a subsequent park).
    pub(crate) fn unpark(&self, _driver: &driver::Handle) {
        self.reactor.unpark(self.worker);
    }
}

/// Guards a thread running a worker of a reactor runtime, calling
/// [`Reactor::worker_start`] on entry and [`Reactor::worker_stop`] on normal
/// exit.
///
/// If a worker thread unwinds (including during `worker_start`), this aborts
/// the process rather than silently dropping `Core` and wedging runtime
/// shutdown or stranding registrations on the dead worker's reactor. Task
/// panics are caught by the task harness and never reach here.
pub(crate) struct RunningWorker {
    worker: usize,
    reactor: Arc<dyn Reactor>,
    started: bool,
}

impl RunningWorker {
    /// To be called at the top of a worker's `run`, before any task executes
    /// on the thread, and held until `run` returns.
    pub(crate) fn start(worker: usize, reactor: Arc<dyn Reactor>) -> Self {
        let mut guard = Self {
            worker,
            reactor,
            started: false,
        };
        guard.reactor.worker_start(worker);
        guard.started = true;
        guard
    }
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "worker {} died by panic; aborting the process: the I/O \
                 reactor it may have been driving cannot be recovered and \
                 the runtime would otherwise hang silently",
                self.worker,
            );
            std::process::abort();
        }
        if self.started {
            self.reactor.worker_stop(self.worker);
        }
    }
}

impl std::fmt::Debug for ReactorParker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReactorParker")
            .field("worker", &self.worker)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReactorUnparker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReactorUnparker")
            .field("worker", &self.worker)
            .finish_non_exhaustive()
    }
}
