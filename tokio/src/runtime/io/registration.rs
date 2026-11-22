#![cfg_attr(not(feature = "net"), allow(dead_code))]

use crate::io::interest::Interest;
use crate::runtime::driver::IoHandle;
use crate::runtime::io::{Direction, ReadyEvent, ScheduledIo};
use crate::runtime::scheduler;

use crate::loom::sync::Arc;
use mio::event::Source;
use std::io;
use std::task::{ready, Context, Poll};

/// Source bound used by [`Registration`].
///
/// When `io-reactor` is enabled on Unix, sources also expose their underlying
/// file descriptor (if fd-backed) so an external [`Reactor`] can watch them
/// without depending on `mio`. Everywhere else this is a zero-cost alias of
/// [`mio::event::Source`].
///
/// [`Reactor`]: crate::runtime::io::reactor::Reactor
pub(crate) trait RegistrationSource: Source {
    #[cfg(all(tokio_unstable, feature = "io-reactor", unix))]
    fn registration_fd(&self) -> Option<std::os::fd::BorrowedFd<'_>>;
}

#[cfg(all(tokio_unstable, feature = "io-reactor", unix))]
mod registration_source_impls {
    use super::RegistrationSource;
    #[cfg(feature = "net")]
    use std::os::fd::AsFd;
    use std::os::fd::BorrowedFd;

    #[cfg(feature = "net")]
    macro_rules! impl_registration_source_via_asfd {
        ($($ty:ty),* $(,)?) => {$(
            impl RegistrationSource for $ty {
                fn registration_fd(&self) -> Option<BorrowedFd<'_>> {
                    Some(self.as_fd())
                }
            }
        )*};
    }

    #[cfg(feature = "net")]
    impl_registration_source_via_asfd! {
        mio::net::TcpStream,
        mio::net::TcpListener,
        mio::net::UdpSocket,
        mio::net::UnixStream,
        mio::net::UnixListener,
        mio::net::UnixDatagram,
        mio::unix::pipe::Sender,
        mio::unix::pipe::Receiver,
    }

    impl RegistrationSource for mio::unix::SourceFd<'_> {
        fn registration_fd(&self) -> Option<BorrowedFd<'_>> {
            let raw = *self.0;
            // Guard against `-1`, which violates `BorrowedFd`'s niche
            // invariant (e.g. if a caller used the deprecated `AsyncFd::new`
            // with an invalid fd).
            if raw < 0 {
                return None;
            }
            // SAFETY: `SourceFd` wraps an fd kept open by its owner across the
            // registration call, and the returned borrow is bounded by `&self`.
            Some(unsafe { BorrowedFd::borrow_raw(raw) })
        }
    }
}

#[cfg(not(all(tokio_unstable, feature = "io-reactor", unix)))]
impl<T: Source> RegistrationSource for T {}

cfg_io_driver! {
    /// Associates an I/O resource with the reactor instance that drives it.
    ///
    /// A registration represents an I/O resource registered with a Reactor such
    /// that it will receive task notifications on readiness. This is the lowest
    /// level API for integrating with a reactor.
    ///
    /// The association between an I/O resource is made by calling
    /// [`new_with_interest_and_handle`].
    /// Once the association is established, it remains established until the
    /// registration instance is dropped.
    ///
    /// A registration instance represents two separate readiness streams. One
    /// for the read readiness and one for write readiness. These streams are
    /// independent and can be consumed from separate tasks.
    ///
    /// **Note**: while `Registration` is `Sync`, the caller must ensure that
    /// there are at most two tasks that use a registration instance
    /// concurrently. One task for [`poll_read_ready`] and one task for
    /// [`poll_write_ready`]. While violating this requirement is "safe" from a
    /// Rust memory safety point of view, it will result in unexpected behavior
    /// in the form of lost notifications and tasks hanging.
    ///
    /// ## Platform-specific events
    ///
    /// `Registration` also allows receiving platform-specific `mio::Ready`
    /// events. These events are included as part of the read readiness event
    /// stream. The write readiness event stream is only for `Ready::writable()`
    /// events.
    ///
    /// [`new_with_interest_and_handle`]: method@Self::new_with_interest_and_handle
    /// [`poll_read_ready`]: method@Self::poll_read_ready`
    /// [`poll_write_ready`]: method@Self::poll_write_ready`
    #[derive(Debug)]
    pub(crate) struct Registration {
        /// Handle to the associated runtime.
        ///
        /// TODO: this can probably be moved into `ScheduledIo`.
        handle: scheduler::Handle,

        /// Reference to state stored by the driver.
        shared: Arc<ScheduledIo>,

        /// Opaque token minted by an external `Reactor` (unused on the `mio`
        /// path).
        #[cfg(all(tokio_unstable, feature = "io-reactor", unix))]
        token: crate::runtime::io::reactor::RegistrationToken,
    }
}

unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

// ===== impl Registration =====

impl Registration {
    /// Registers the I/O resource with the reactor for the provided handle, for
    /// a specific `Interest`. This does not add `hup` or `error` so if you are
    /// interested in those states, you will need to add them to the readiness
    /// state passed to this function.
    ///
    /// # Return
    ///
    /// - `Ok` if the registration happened successfully
    /// - `Err` if an error was encountered during registration
    #[track_caller]
    pub(crate) fn new_with_interest_and_handle(
        io: &mut impl RegistrationSource,
        interest: Interest,
        handle: scheduler::Handle,
    ) -> io::Result<Registration> {
        match &handle.driver().io {
            IoHandle::Enabled(driver) => {
                let shared = driver.add_source(io, interest)?;
                Ok(Registration {
                    handle,
                    shared,
                    #[cfg(all(tokio_unstable, feature = "io-reactor", unix))]
                    token: crate::runtime::io::reactor::RegistrationToken::default(),
                })
            }
            #[cfg(all(tokio_unstable, feature = "io-reactor", unix))]
            IoHandle::Reactor(reactor) => {
                let (shared, token) = reactor.register(io.registration_fd(), interest)?;
                Ok(Registration {
                    handle,
                    shared,
                    token,
                })
            }
            IoHandle::Disabled(_) => {
                panic!(
                    "A Tokio 1.x context was found, but IO is disabled. \
                     Call `enable_io` on the runtime builder to enable IO."
                );
            }
        }
    }

    /// Deregisters the I/O resource from the reactor it is associated with.
    ///
    /// This function must be called before the I/O resource associated with the
    /// registration is dropped.
    ///
    /// Note that deregistering does not guarantee that the I/O resource can be
    /// registered with a different reactor. Some I/O resource types can only be
    /// associated with a single reactor instance for their lifetime.
    ///
    /// # Return
    ///
    /// If the deregistration was successful, `Ok` is returned. Any calls to
    /// `Reactor::turn` that happen after a successful call to `deregister` will
    /// no longer result in notifications getting sent for this registration.
    ///
    /// `Err` is returned if an error is encountered.
    pub(crate) fn deregister(&mut self, io: &mut impl RegistrationSource) -> io::Result<()> {
        match &self.handle.driver().io {
            IoHandle::Enabled(driver) => driver.deregister_source(&self.shared, io),
            #[cfg(all(tokio_unstable, feature = "io-reactor", unix))]
            IoHandle::Reactor(reactor) => reactor.deregister(self.token),
            IoHandle::Disabled(_) => {
                unreachable!("Registration cannot be created with IO disabled")
            }
        }
    }

    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        self.shared.clear_readiness(event);
    }

    /// Marks the resource ready without waiting for the driver's first event.
    /// Used for sockets whose state is known when they are created: accepted
    /// sockets are writable and usually already hold the peer's first bytes
    /// (under load that first event can queue behind every established
    /// connection's events), and both ends of a fresh `pair()` are writable.
    /// A wrong guess costs one `WouldBlock`, which clears the readiness again.
    #[cfg(feature = "net")]
    pub(crate) fn assume_ready(&self, ready: crate::io::Ready) {
        self.shared.assume_ready(ready);
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    pub(crate) fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<ReadyEvent>> {
        self.poll_ready(cx, Direction::Read)
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    pub(crate) fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<ReadyEvent>> {
        self.poll_ready(cx, Direction::Write)
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    #[cfg(not(all(target_os = "wasi", target_env = "p1")))]
    pub(crate) fn poll_read_io<R>(
        &self,
        cx: &mut Context<'_>,
        f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Direction::Read, f)
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    pub(crate) fn poll_write_io<R>(
        &self,
        cx: &mut Context<'_>,
        f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Direction::Write, f)
    }

    /// Polls for events on the I/O resource's `direction` readiness stream.
    ///
    /// If called with a task context, notify the task when a new event is
    /// received.
    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
    ) -> Poll<io::Result<ReadyEvent>> {
        ready!(crate::trace::trace_leaf(cx));
        // Keep track of task budget
        let coop = ready!(crate::task::coop::poll_proceed(cx));
        let ev = ready!(self.shared.poll_readiness(cx, direction));

        if ev.is_shutdown {
            return Poll::Ready(Err(gone()));
        }

        coop.made_progress();
        Poll::Ready(Ok(ev))
    }

    fn poll_io<R>(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
        mut f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        loop {
            let ev = ready!(self.poll_ready(cx, direction))?;

            match f() {
                Ok(ret) => {
                    return Poll::Ready(Ok(ret));
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_readiness(ev);
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    pub(crate) fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        let ev = self.shared.ready_event(interest);

        // Don't attempt the operation if the resource is not ready.
        if ev.ready.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        match f() {
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.clear_readiness(ev);
                Err(io::ErrorKind::WouldBlock.into())
            }
            res => res,
        }
    }

    pub(crate) async fn readiness(&self, interest: Interest) -> io::Result<ReadyEvent> {
        let ev = self.shared.readiness(interest).await;

        if ev.is_shutdown {
            return Err(gone());
        }

        Ok(ev)
    }

    pub(crate) async fn async_io<R>(
        &self,
        interest: Interest,
        mut f: impl FnMut() -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            let event = self.readiness(interest).await?;

            let coop = std::future::poll_fn(crate::task::coop::poll_proceed).await;

            match f() {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_readiness(event);
                }
                x => {
                    coop.made_progress();
                    return x;
                }
            }
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // It is possible for a cycle to be created between wakers stored in
        // `ScheduledIo` instances and `Arc<driver::Inner>`. To break this
        // cycle, wakers are cleared. This is an imperfect solution as it is
        // possible to store a `Registration` in a waker. In this case, the
        // cycle would remain.
        //
        // See tokio-rs/tokio#3481 for more details.
        self.shared.clear_wakers();
    }
}

fn gone() -> io::Error {
    io::Error::other(crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR)
}
