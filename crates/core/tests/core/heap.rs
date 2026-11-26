//! A counting global allocator for the memory gates.
//!
//! A test binary installs [`Counting`] as its `#[global_allocator]` and wraps
//! the code under test in [`peak_of`], which reports the most heap the closure
//! held at once beyond what was already live. That is what lets a test say
//! "restore holds one chunk, whatever the database's size" as a number.
//!
//! Counts are per thread, so tests running side by side in one process do not
//! disturb each other's measurement; the closure must therefore do its
//! allocating on the calling thread (every gated path is driven by
//! `block_on`, which does). The counters are `const`-initialised cells with no
//! destructor, so touching them from inside the allocator never allocates.
//!
//! A path that allocates on threads of its own (an HTTP client's batch workers) is
//! measured with [`process_peak_of`], which counts every thread of the process but the
//! ones that called [`exclude_thread`] (a test server standing in for the far end of
//! the wire, whose buffers are not the code under test's). Process-wide counts are
//! only sound when nothing else in the process allocates meanwhile, which is how
//! nextest runs a test (one process each).
//!
//! The file is shared: the core and store-fs test binaries include it and the store-s3 one
//! includes it by path, each using part of it.
//!
//! This is the one `unsafe` outside the SQLite crate: `GlobalAlloc` cannot be
//! implemented without it. It forwards every call to the system allocator and
//! only does arithmetic on the side. It lives in this test binary, so it is
//! never linked into a shipped one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};

thread_local! {
    /// Bytes this thread has allocated and not yet freed (negative when it frees
    /// what another thread allocated).
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// The highest `LIVE` since the last [`peak_of`] began.
    static PEAK: Cell<isize> = const { Cell::new(0) };
    /// This thread's allocations are not the code under test's: the process-wide
    /// count skips them.
    static EXCLUDED: Cell<bool> = const { Cell::new(false) };
    /// Allocations (and growths by realloc) of [`LARGE_BLOCK`] or more on this thread.
    static LARGE: Cell<usize> = const { Cell::new(0) };
}

/// Bytes the process has allocated and not yet freed on threads that are not excluded.
static PROCESS_LIVE: AtomicIsize = AtomicIsize::new(0);
/// The highest `PROCESS_LIVE` since the last [`process_peak_of`] began.
static PROCESS_PEAK: AtomicIsize = AtomicIsize::new(0);

/// Leaves the calling thread out of [`process_peak_of`] for good.
pub fn exclude_thread() {
    EXCLUDED.with(|excluded| excluded.set(true));
}
/// The allocator to install: `#[global_allocator] static HEAP: Counting = Counting;`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Counting;

/// Adds `delta` bytes to this thread's live count and raises its peak.
fn count(delta: isize) {
    // `try_with`: a thread being torn down has no cell left, and the allocator
    // must not panic then, so a missing cell is a count that does not matter.
    if let Err(_ending) = LIVE.try_with(|live| {
        let now = live.get().saturating_add(delta);
        live.set(now);
        if let Err(_ending) = PEAK.try_with(|peak| peak.set(peak.get().max(now))) {}
    }) {}
    if !EXCLUDED.try_with(Cell::get).unwrap_or(true) {
        let now = PROCESS_LIVE
            .fetch_add(delta, Ordering::Relaxed)
            .saturating_add(delta);
        PROCESS_PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

/// The size from which an allocation counts as large for [`large_allocs_of`]: blocks the system
/// allocator takes whole, and on macOS keeps mapped after the free.
pub const LARGE_BLOCK: usize = 256 * 1024;

/// Counts a large allocation of `size` bytes on this thread.
fn count_large(size: usize) {
    if size >= LARGE_BLOCK
        && let Err(_ending) = LARGE.try_with(|large| large.set(large.get().saturating_add(1)))
    {}
}

/// A layout's size as a signed count; sizes above `isize::MAX` cannot be allocated.
fn signed(size: usize) -> isize {
    isize::try_from(size).unwrap_or(isize::MAX)
}

#[expect(
    unsafe_code,
    reason = "GlobalAlloc is an unsafe trait; every method forwards to System unchanged"
)]
// SAFETY: each method forwards its arguments to `System` and returns its answer, so
// `System`'s guarantees are this allocator's; the counting touches only thread-local cells.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract, which is `System`'s.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            count(signed(layout.size()));
            count_large(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract, which is `System`'s.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(signed(layout.size()));
            count_large(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller passes a pointer from this allocator with this layout, as `System` needs.
        unsafe { System.dealloc(ptr, layout) };
        count(signed(layout.size()).saturating_neg());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract, which is `System`'s.
        let moved = unsafe { System.realloc(ptr, layout, new_size) };
        if !moved.is_null() {
            count(signed(new_size).saturating_sub(signed(layout.size())));
            if new_size > layout.size() {
                count_large(new_size);
            }
        }
        moved
    }
}

/// How much heap a closure used at its high-water mark.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Peak {
    /// The most bytes live at once beyond those live when the closure began.
    pub extra: usize,
    /// Bytes still live when it returned beyond those live at the start (what
    /// the result holds, for the closure's caller to see).
    pub kept: usize,
}

/// Runs `f` and reports its peak heap on this thread.
///
/// `f`'s result is returned alongside, still live: a result's size is part of
/// the peak whenever it was built before the high-water mark.
pub fn peak_of<R>(f: impl FnOnce() -> R) -> (R, Peak) {
    let start = LIVE.with(Cell::get);
    PEAK.with(|peak| peak.set(start));
    let result = f();
    let high = PEAK.with(Cell::get);
    let end = LIVE.with(Cell::get);
    let above = |mark: isize| usize::try_from(mark.saturating_sub(start)).unwrap_or(0);
    (
        result,
        Peak {
            extra: above(high),
            kept: above(end),
        },
    )
}

/// Runs `f` and counts the allocations of [`LARGE_BLOCK`] or more it made on this thread,
/// growths of a buffer included: each is a block the allocator may keep after the free, so a
/// path that allocates one per unit of work, instead of holding one, shows here.
pub fn large_allocs_of<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = LARGE.with(Cell::get);
    let result = f();
    (result, LARGE.with(Cell::get).saturating_sub(before))
}

/// [`peak_of`] over every thread of the process except the excluded ones: the closure
/// may allocate on workers it starts, and `extra` is the most the process held at once.
///
/// Memory the closure's threads free that an excluded thread allocated would read as
/// negative, so the closure must not take buffers from the excluded side.
pub fn process_peak_of<R>(f: impl FnOnce() -> R) -> (R, Peak) {
    let start = PROCESS_LIVE.load(Ordering::Relaxed);
    PROCESS_PEAK.store(start, Ordering::Relaxed);
    let result = f();
    let high = PROCESS_PEAK.load(Ordering::Relaxed);
    let end = PROCESS_LIVE.load(Ordering::Relaxed);
    let above = |mark: isize| usize::try_from(mark.saturating_sub(start)).unwrap_or(0);
    (
        result,
        Peak {
            extra: above(high),
            kept: above(end),
        },
    )
}
