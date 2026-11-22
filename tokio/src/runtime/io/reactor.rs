//! The seam for an I/O reactor that lives outside Tokio.
//!
//! A runtime built with [`Builder::io_reactor`] hands two jobs to a
//! [`Reactor`]: watching the runtime's I/O resources for readiness, and
//! putting worker threads to sleep. Tokio's built-in `mio` driver is not
//! constructed or used. Timers stay with Tokio and are exposed through
//! [`Timers`] during [`Reactor::park`].
//!
//! [`Builder::io_reactor`]: crate::runtime::Builder::io_reactor

use crate::io::{Interest, Ready};
use crate::loom::sync::Arc;
use crate::runtime::driver;
use crate::runtime::io::{IoDriverMetrics, ScheduledIo};

use std::fmt;
use std::io;
use std::os::fd::BorrowedFd;
use std::time::Duration;

/// Opaque per-registration token minted by [`Reactor::register`] and passed
/// back to [`Reactor::deregister`].
///
/// Tokio stores this value alongside the registration and never inspects it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct RegistrationToken(pub u64);

/// Whether a call to [`Reactor::park`] may put the calling thread to sleep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParkMode {
    /// Non-blocking poll (for example, a periodic maintenance check on a busy
    /// worker). Must not sleep: report any readiness that is available now and
    /// return.
    Poll,
    /// Blocking park, optionally bounded by a scheduler-supplied timeout (for
    /// example, from per-worker timers under `TimerFlavor::Alternative`).
    ///
    /// A pending [`Reactor::unpark`] must make this return without sleeping.
    Park(Option<Duration>),
}

/// Access to Tokio's shared timer wheel during [`Reactor::park`].
///
/// When a runtime has the traditional shared timer driver enabled, the reactor
/// must ensure that all workers never sleep simultaneously without at least
/// one sleeping worker having bounded its sleep by [`Timers::next_timeout`].
///
/// To avoid lost timer wakeups, a worker that takes timer-keeping duty must
/// publish itself as reachable by [`Reactor::unpark_for_timer`] with `SeqCst`
/// ordering **before** calling [`Timers::next_timeout`]. Because timer
/// insertions update the atomic deadline with `SeqCst` before calling
/// [`Reactor::unpark_for_timer`], the total order guarantees that a concurrent
/// insertion either wakes that worker or is observed by `next_timeout`.
///
/// Reactors do not need to advance the timer wheel themselves: Tokio fires any
/// expired timers automatically after [`Reactor::park`] returns.
pub struct Timers<'a> {
    driver: Option<&'a driver::Handle>,
}

impl<'a> Timers<'a> {
    pub(crate) fn new(driver: &'a driver::Handle) -> Self {
        Self {
            driver: Some(driver),
        }
    }

    /// A handle that reports no shared timers (used for non-blocking polls and
    /// when `time::Driver` has already handled paused-clock auto-advance).
    pub(crate) fn none() -> Self {
        Self { driver: None }
    }

    /// Returns how long until the next pending timer expires, or `None` if no
    /// timers are registered (or the runtime does not use the shared timer
    /// wheel).
    ///
    /// Returns `Some(Duration::ZERO)` if a timer is already due, in which case
    /// the caller should return without sleeping so Tokio can fire it.
    ///
    /// This method is lock-free.
    pub fn next_timeout(&self) -> Option<Duration> {
        #[cfg(feature = "time")]
        {
            let driver = self.driver?;
            let time = traditional_timer(driver)?;
            let when = time.next_wake_tick()?;
            let now = time.time_source().now(driver.clock());
            Some(
                time.time_source()
                    .tick_to_duration(when.saturating_sub(now)),
            )
        }
        #[cfg(not(feature = "time"))]
        {
            let _ = self.driver;
            None
        }
    }
}

impl fmt::Debug for Timers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timers").finish_non_exhaustive()
    }
}

/// An I/O reactor that drives a runtime in place of Tokio's built-in driver.
///
/// # Registrations
///
/// Tokio's sockets, pipes and [`AsyncFd`]s register with the reactor through
/// [`register`](Self::register), from any thread. The reactor answers with a
/// [`Readiness`] and a [`RegistrationToken`], and from then on reports
/// readiness by calling [`Readiness::set_ready`].
///
/// # Parking
///
/// A worker with nothing to run (or performing periodic maintenance) calls
/// [`park`](Self::park). Workers are numbered from zero, and the factory given
/// to [`Builder::io_reactor`] is told how many there are. A `current_thread`
/// runtime has the single worker `0`.
///
/// `park` is called for a given worker by one thread at a time, and that thread
/// is the one running the worker. Which thread that is can change:
/// [`block_in_place`] moves a worker to another thread (unless
/// [`allow_block_in_place`](Self::allow_block_in_place) returns `false`), and
/// on a `current_thread` runtime the worker runs on whichever thread is in
/// `block_on`.
///
/// [`AsyncFd`]: crate::io::unix::AsyncFd
/// [`Builder::io_reactor`]: crate::runtime::Builder::io_reactor
/// [`block_in_place`]: crate::task::block_in_place
pub trait Reactor: Send + Sync + fmt::Debug + 'static {
    /// Starts watching the file behind `fd` for `interest`.
    ///
    /// `fd` is only valid for the length of this call, and its owner may close
    /// it as soon as the call returns. A reactor that reaches the kernel later
    /// must duplicate it now ([`BorrowedFd::try_clone_to_owned`]) and work with
    /// the duplicate.
    ///
    /// An error returned here reaches the caller that was creating the
    /// resource. A failure that shows up later has no caller to go to: report
    /// it with [`Readiness::shutdown`], which fails everything waiting on the
    /// resource.
    fn register(
        &self,
        fd: BorrowedFd<'_>,
        interest: Interest,
    ) -> io::Result<(Readiness, RegistrationToken)>;

    /// Ends the registration identified by `token`, which
    /// [`register`](Self::register) returned for it.
    ///
    /// Nothing waits on the registration's [`Readiness`] any more, so a report
    /// that is still on its way does no harm, and the reactor may undo the
    /// registration asynchronously. The resource's owner may close its
    /// descriptor immediately after this returns, so any deferred kernel
    /// cleanup must not depend on it.
    fn deregister(&self, token: RegistrationToken) -> io::Result<()>;

    /// Polls or parks `worker`, reporting readiness in the meantime. Returns
    /// whether this call drove the reactor, as opposed to waiting for another
    /// thread that did. May return early.
    ///
    /// When `mode` is [`ParkMode::Poll`], this must not block.
    /// When `mode` is [`ParkMode::Park`], an [`unpark`](Self::unpark) that
    /// arrived before the call must make it return without sleeping.
    ///
    /// See [`Timers`] for how to bound sleeps by Tokio's shared timer wheel.
    fn park(&self, worker: usize, mode: ParkMode, timers: &Timers<'_>) -> bool;

    /// Wakes `worker` if it is parked, and otherwise makes its next blocking
    /// [`park`](Self::park) return without sleeping. Called from any thread,
    /// the worker's own included.
    fn unpark(&self, worker: usize);

    /// Called when a timer is inserted that expires sooner than the current
    /// earliest deadline. Must wake at least one worker bound by
    /// [`Timers::next_timeout`] so it can re-park with the shorter timeout.
    ///
    /// Called without holding Tokio's timer wheel lock.
    fn unpark_for_timer(&self);

    /// Whether a multi-threaded worker driven by this reactor may be handed off
    /// to another OS thread by [`block_in_place`].
    ///
    /// Reactors that bind per-worker state to a single OS thread (for example,
    /// `io_uring` rings created with `IORING_SETUP_SINGLE_ISSUER`) should
    /// return `false`. Calling [`block_in_place`] on a worker of such a
    /// runtime then panics synchronously on the calling thread before the
    /// worker core is moved.
    ///
    /// Defaults to `true`.
    ///
    /// [`block_in_place`]: crate::task::block_in_place
    fn allow_block_in_place(&self) -> bool {
        true
    }

    /// Optional hook called on a multi-threaded worker when its local queue
    /// runs empty, just before it attempts to steal tasks from other workers.
    ///
    /// Runs inside the Tokio runtime context with the worker's core available,
    /// so tasks woken here are scheduled onto this worker's local queue and run
    /// instead of stealing. This lets a per-worker reactor flush submissions
    /// and reap local completions before reaching for other workers' queues.
    ///
    /// Defaults to a no-op.
    fn before_steal(&self, worker: usize) {
        let _ = worker;
    }

    /// Called on a thread that is about to run `worker`, before any task runs
    /// there: when the runtime starts, and again each time [`block_in_place`]
    /// moves the worker to a new thread. Not called on a `current_thread`
    /// runtime.
    ///
    /// Note: when [`allow_block_in_place`](Self::allow_block_in_place) is
    /// `true`, `worker_start(w)` on the replacement thread may execute before
    /// `worker_stop(w)` executes on the thread still running the blocking
    /// closure.
    ///
    /// [`block_in_place`]: crate::task::block_in_place
    fn worker_start(&self, worker: usize) {
        let _ = worker;
    }

    /// Called on a thread that has stopped running `worker`, once for every
    /// [`worker_start`](Self::worker_start).
    fn worker_stop(&self, worker: usize) {
        let _ = worker;
    }

    /// Called when the runtime shuts down, possibly more than once.
    ///
    /// Reactors should call [`Readiness::shutdown`] on every still-registered
    /// resource so waiters observe a runtime-shutdown error instead of waiting
    /// forever.
    fn shutdown(&self) {}
}

/// The readiness of one registered resource, shared between the reactor that
/// reports it and the Tokio resource that waits for it. Clones refer to the
/// same state.
#[derive(Clone, Debug)]
pub struct Readiness {
    io: Arc<ScheduledIo>,
}

impl Readiness {
    /// The state of a resource that is not ready for anything yet.
    pub fn new() -> Readiness {
        Readiness {
            io: Arc::new(ScheduledIo::default()),
        }
    }

    /// Reports that the resource has become ready for `ready`, in addition to
    /// what it was ready for, and wakes tasks waiting for that.
    ///
    /// Readiness is cleared by the resource's users when an operation finds it
    /// not ready after all. A report that turns out to be spurious costs one
    /// failed attempt, while a missed one leaves tasks waiting.
    ///
    /// Safe to call concurrently with or after [`shutdown`](Self::shutdown), in
    /// which case this is a no-op.
    pub fn set_ready(&self, ready: Ready) {
        if self.io.try_set_readiness(ready) {
            self.io.wake(ready);
        }
    }

    /// Marks the resource as one that will get no more reports, which fails
    /// current and future waits on it with an error. For a registration the
    /// reactor can no longer serve, and for all of them when the runtime shuts
    /// down.
    pub fn shutdown(&self) {
        self.io.shutdown();
    }
}

impl Default for Readiness {
    fn default() -> Readiness {
        Readiness::new()
    }
}

/// Crate-internal handle stored in `driver::IoHandle::Reactor`.
pub(crate) struct ReactorHandle {
    reactor: std::sync::Arc<dyn Reactor>,
    metrics: IoDriverMetrics,
}

impl ReactorHandle {
    pub(crate) fn new(reactor: std::sync::Arc<dyn Reactor>) -> Self {
        Self {
            reactor,
            metrics: IoDriverMetrics::default(),
        }
    }

    pub(crate) fn reactor(&self) -> &std::sync::Arc<dyn Reactor> {
        &self.reactor
    }

    pub(crate) fn metrics(&self) -> &IoDriverMetrics {
        &self.metrics
    }

    pub(crate) fn register(
        &self,
        fd: Option<BorrowedFd<'_>>,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, RegistrationToken)> {
        let fd = fd.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "this runtime's I/O reactor needs a source backed by a file descriptor",
            )
        })?;
        let (readiness, token) = self.reactor.register(fd, interest)?;
        self.metrics.incr_fd_count();
        Ok((readiness.io, token))
    }

    pub(crate) fn deregister(&self, token: RegistrationToken) -> io::Result<()> {
        let res = self.reactor.deregister(token);
        if res.is_ok() {
            self.metrics.dec_fd_count();
        }
        res
    }
}

impl fmt::Debug for ReactorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactorHandle")
            .field("reactor", &self.reactor)
            .finish_non_exhaustive()
    }
}

/// Parks or polls `worker` on `reactor`, advancing Tokio's traditional timer
/// wheel on return if any timer is due. Returns what [`Reactor::park`]
/// returned.
pub(crate) fn park(
    reactor: &dyn Reactor,
    driver: &driver::Handle,
    worker: usize,
    mode: ParkMode,
) -> bool {
    let timers = match mode {
        ParkMode::Poll => Timers::none(),
        ParkMode::Park(_) => Timers::new(driver),
    };
    let drove = reactor.park(worker, mode, &timers);
    #[cfg(feature = "time")]
    if let Some(time) = traditional_timer(driver) {
        time.parker_process(driver.clock());
    }
    drove
}

/// The time driver, if the runtime has one whose single wheel is advanced by
/// whoever parks. The alternative timer keeps a wheel per worker, which the
/// scheduler advances itself.
#[cfg(feature = "time")]
fn traditional_timer(driver: &driver::Handle) -> Option<&crate::runtime::time::Handle> {
    driver
        .time_handle_opt()
        .filter(|time| time.is_traditional())
}
