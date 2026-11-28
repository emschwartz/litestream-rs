//! A restore opens its next small files before it reaches them (`restore::window::Lookahead`):
//! their requests are in flight together, none past Go's bound of 32 ahead, one request a file,
//! and the pages are the ones a restore that opens each file at its turn gives.

use super::super::window::{AHEAD_BYTES, LOOKAHEAD, Lookahead};
use super::*;
use crate::layout::ListedTime;
use crate::store::SPAN_TO_END;

/// A snapshot and `incrementals` one-transaction files after it, each setting page 2.
fn chain(incrementals: u64) -> MemStore {
    let store = MemStore::new();
    put_spec(
        &store,
        9,
        &Spec::plain(512, 3, 1, 1, &[(1, 1), (2, 2), (3, 3)]),
    );
    for n in 2..=incrementals + 1 {
        let seed = u8::try_from(n).unwrap();
        put_spec(&store, 0, &Spec::plain(512, 3, n, n, &[(2, seed)]));
    }
    store
}

fn wide(inner: MemStore, width: usize) -> Wrapped {
    let mut store = Wrapped::new(inner);
    store.width = width;
    store
}

#[test]
fn the_next_files_are_opened_before_the_first_is_read() {
    let files = u64::try_from(LOOKAHEAD).unwrap() + 8;
    let store = wide(chain(files - 1), 2);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let restored = result.unwrap();
    assert_eq!(restored.files(), usize::try_from(files).unwrap());
    let spans = store.spans.lock().unwrap().clone();
    // One request a file, each to the object's end; the snapshot, the first file, is read as the
    // base before any file is opened ahead.
    assert_eq!(spans.len(), usize::try_from(files).unwrap() - 1);
    assert!(
        spans
            .iter()
            .all(|(_, start, end, _)| (*start, *end) == (0, SPAN_TO_END))
    );
    // The first LOOKAHEAD are in flight before any more is read, and none is opened further
    // ahead than that.
    let first = spans[0].3;
    for (index, (_, _, _, begun)) in spans.iter().enumerate() {
        let consumed = index.saturating_sub(LOOKAHEAD - 1);
        assert!(
            *begun >= first + consumed,
            "file {index} opened after {begun} reads"
        );
        if index < LOOKAHEAD {
            assert_eq!(*begun, first, "file {index} opened ahead of the first read");
        }
    }
    assert_eq!(
        seeds(&sink.published().unwrap())[1],
        u8::try_from(files).unwrap()
    );
}

#[test]
fn a_store_that_reads_one_stream_at_a_time_opens_nothing_ahead() {
    let alone = wide(chain(5), 1);
    let (result, sink) = run(&alone, RestoreTarget::Latest);
    let reference = result.unwrap();
    assert!(alone.spans.lock().unwrap().is_empty());
    let many = wide(chain(5), 3);
    let (result, ahead) = run(&many, RestoreTarget::Latest);
    let ahead_result = result.unwrap();
    assert_eq!(many.spans.lock().unwrap().len(), 5);
    assert_eq!(ahead.published(), sink.published());
    assert_eq!(ahead_result.bytes_read(), reference.bytes_read());
    assert_eq!(ahead_result.files(), reference.files());
}

fn info(index: u64, size: u64) -> FileInfo {
    FileInfo {
        key: key(0, index, index),
        size,
        listed: ListedTime::LastModified(Timestamp::from_unix_millis(0)),
    }
}

#[test]
fn a_file_is_opened_ahead_up_to_the_size_limit_and_no_larger() {
    let store = wide(chain(0), 2);
    let files = [info(1, 10), info(2, 11), info(3, 10), info(4, 0)];
    let mut ahead = Lookahead::with(&store, &files, 0, 4, 10);
    let (index, _, _) = ahead.next_file().unwrap();
    assert_eq!(index, 0);
    let opened: Vec<FileKey> = store.spans.lock().unwrap().iter().map(|s| s.0).collect();
    assert_eq!(opened, [files[0].key, files[2].key, files[3].key]);
    let rest: Vec<usize> = std::iter::from_fn(|| ahead.next_file().map(|f| f.0)).collect();
    assert_eq!(rest, [1, 2, 3]);
    assert_eq!(
        store.spans.lock().unwrap().len(),
        3,
        "each file opened once"
    );
}

#[test]
fn files_are_opened_no_further_ahead_than_the_depth() {
    let store = wide(chain(0), 2);
    let files: Vec<FileInfo> = (1..=6).map(|n| info(n, 1)).collect();
    let mut ahead = Lookahead::with(&store, &files, 2, 3, AHEAD_BYTES);
    for (taken, want) in [(1, 3), (2, 4), (3, 4), (4, 4)] {
        ahead.next_file().unwrap();
        assert_eq!(store.spans.lock().unwrap().len(), want, "after {taken}");
    }
    assert!(ahead.next_file().is_none());
    // Started at the third file: the first two are never opened.
    let first = store.spans.lock().unwrap()[0].0;
    assert_eq!(first, files[2].key);
}

#[test]
fn no_depth_opens_nothing_ahead() {
    let store = wide(chain(0), 2);
    let files = [info(1, 1), info(2, 1)];
    let mut ahead = Lookahead::with(&store, &files, 0, 0, AHEAD_BYTES);
    assert_eq!(ahead.next_file().map(|f| f.0), Some(0));
    assert_eq!(ahead.next_file().map(|f| f.0), Some(1));
    assert!(ahead.next_file().is_none());
    assert!(store.spans.lock().unwrap().is_empty());
}

#[test]
fn the_default_limit_is_a_megabyte() {
    let store = wide(chain(0), 2);
    let files = [info(1, 1), info(2, AHEAD_BYTES), info(3, AHEAD_BYTES + 1)];
    let mut ahead = Lookahead::new(&store, &files, 0);
    ahead.next_file().unwrap();
    let opened: Vec<FileKey> = store.spans.lock().unwrap().iter().map(|s| s.0).collect();
    assert_eq!(opened, [files[0].key, files[1].key]);
    assert_eq!(AHEAD_BYTES, 1 << 20);
}
