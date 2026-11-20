use std::cell::Cell;
use std::pin::Pin;

use super::*;

/// Pends `n` times, then returns `n`.
struct Pends {
    left: u32,
    total: u32,
}

impl Future for Pends {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u32> {
        if self.left == 0 {
            return Poll::Ready(self.total);
        }
        self.left = self.left.saturating_sub(1);
        Poll::Pending
    }
}

fn pends(n: u32) -> Pends {
    Pends { left: n, total: n }
}

#[test]
fn a_run_inside_its_budget_finishes() {
    assert_eq!(run_until(pends(3), Polls::new(3)), RunEnd::Done(3));
    assert_eq!(run_until(pends(3), Polls::UNBOUNDED), RunEnd::Done(3));
    assert_eq!(run_until(pends(0), Polls::new(0)), RunEnd::Done(0));
}

#[test]
fn a_run_past_its_budget_is_dropped_at_the_next_pending() {
    assert_eq!(
        run_until(pends(3), Polls::new(2)),
        RunEnd::Crashed { polls: 2 }
    );
    assert_eq!(
        run_until(pends(1), Polls::new(0)),
        RunEnd::Crashed { polls: 0 }
    );
}

#[test]
fn count_polls_counts_every_pending() {
    assert_eq!(count_polls(pends(4)), (4, 4));
    assert_eq!(count_polls(pends(0)), (0, 0));
}

#[test]
fn the_crash_drops_what_the_future_holds() {
    struct Guard<'a>(&'a Cell<bool>);
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let dropped = Cell::new(false);
    let held = async {
        let _guard = Guard(&dropped);
        pends(5).await
    };
    assert_eq!(run_until(held, Polls::new(1)), RunEnd::Crashed { polls: 1 });
    assert!(dropped.get());
}
