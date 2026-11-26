//! The replica store under the wrappers: the simulator's `MemStore`, or the real S3 store
//! (`lsr-store-s3`) over its in-memory bucket (`FakeS3`) behind its recorded-fault layer.
//!
//! The engine reaches the replica through [`RemoteBackend`] under the world's own wrappers (the
//! object-level faults, the yield on each call, the recorder), so the drawn store faults, the
//! corrupting publish and the put log work on either backend. With [`RemoteBackend::S3`] every
//! call the engine makes is a signed HTTP request the store builds, sends through
//! `FaultTransport` and classifies, and an HTTP fault drawn by the history
//! ([`crate::Op::HttpFault`]) is pushed onto that transport.
//!
//! The harness reads, lists and edits the replica through [`Bucket`], never through the
//! engine's client: with S3 it reads the objects the bucket stores (keys, bodies, created-at
//! metadata, last-modified times) and answers as `MemStore` would over the same objects, so the
//! judges see what the bucket holds even when the client would read it wrongly. Keys follow the replica layout (`<prefix>/<level as 4 hex digits>/<name>`), parsed
//! here from the bucket's names.
//!
//! # Parts
//!
//! At its own part size the store sends every file here in one PUT: the files are a few hundred
//! bytes and a part is 5 MiB. With [`Parts`] the store is built with a part of a few dozen
//! bytes over a bucket that takes them, and its parts go to lanes the simulator drives
//! ([`Landing`], over `lsr_store_s3::Manual`), so a multipart upload is a deterministic
//! sequence of requests the run's seed orders. The law, from what the S3 API says of
//! UploadPart (each part is its own request; parts may be sent in any order and in parallel;
//! a part's request may fail while the others succeed):
//!
//! - A part the store started stays started until the engine next asks its upload about its
//!   parts, or the store waits for one itself. At each question the seed lands none, some or
//!   all of the parts started, in an order it draws; never none twice running, so an upload
//!   always ends. Until a part lands the engine sees nothing of it, so landing at the
//!   question is every order of events the engine could tell apart, without a clock.
//! - A landing whose request met a provider fault ends that question's landings: one fault
//!   fires for one question, and the engine's report of that tick is judged against it.
//! - When the process ends (a stop, a kill, a power loss), each part still started either
//!   reached the bucket or was never sent, by the seed; and what the store remembered of
//!   uploads to abort is gone, so the next boot has only the bucket's listing.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::conformance::block_on;
use lsr_core::{
    BatchKind, Clock, CreatedAt, Entropy, FakeClock, FileInfo, FileKey, Flight, Level, ListAt,
    Listing, MemStore, ObjectStore, PageLimit, ReadLen, Reread, Rest, SeededEntropy, StoreError,
    Upload, WriteMode, parse_file_name,
};
use lsr_ltx::{Timestamp, TxidRange};
use lsr_store_s3::{
    BucketName, Endpoint, FakeS3, Fault, FaultTransport, HttpRequest, HttpResponse, Inline, Lanes,
    Manual, PartSize, Prefix, S3Config, S3Credentials, S3Store, S3Upload, SecretString, Shape,
    Transport, TransportError,
};

use crate::generate::{DenialCode, HttpFault, HttpShape};

/// The bucket's fault layer, shared by the store that owns a transport and the harness that
/// scripts faults and reads the bucket.
pub type Wire = Arc<FaultTransport<FakeS3>>;

/// The store's transport: the shared [`Wire`], and the lanes its parts go to when the run
/// sends in parts.
#[derive(Debug, Clone)]
pub struct WireTransport {
    wire: Wire,
    lanes: Option<Arc<Manual>>,
}

impl Transport for WireTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.wire.send(request)
    }

    fn lanes(&self, _most: NonZeroUsize) -> Arc<dyn Lanes> {
        match &self.lanes {
            Some(lanes) => Arc::<Manual>::clone(lanes),
            None => Arc::new(Inline),
        }
    }
}

/// How the S3 replica sends in parts (module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Parts {
    /// The bytes of a part.
    pub part: NonZeroU64,
    /// How many parts of one upload may be started at once.
    pub lanes: NonZeroUsize,
}

impl Parts {
    /// A part of `part` bytes, `lanes` at once; `None` for a zero.
    #[must_use]
    pub fn new(part: u64, lanes: usize) -> Option<Self> {
        Some(Self {
            part: NonZeroU64::new(part)?,
            lanes: NonZeroUsize::new(lanes)?,
        })
    }
}

/// What became of the parts of a run, for its reach counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartCounts {
    /// Parts that landed at a question or a wait.
    pub landed: u64,
    /// Of those, the ones that landed ahead of a part started before them.
    pub out_of_order: u64,
    /// Of those, the ones whose request met a provider fault.
    pub faulted: u64,
    /// Questions that left a part started.
    pub left_started: u64,
    /// Questions the upload answered with every lane taken and a part waiting.
    pub full: u64,
    /// Parts that reached the bucket as their process ended.
    pub landed_at_exit: u64,
    /// Parts never sent because their process ended.
    pub lost_at_exit: u64,
}

/// What the lanes' own pick and the handle share.
struct Lands {
    chance: SeededEntropy,
    /// A scenario lands the parts itself: a question lands none.
    by_hand: AtomicBool,
    /// The last question landed nothing with a part started.
    landed_none: AtomicBool,
    counts: Mutex<PartCounts>,
}

impl Lands {
    /// One of `started`, counted from zero.
    fn one_of(&self, started: usize) -> usize {
        let upper = u64::try_from(started).unwrap_or(u64::MAX).max(1);
        usize::try_from(self.chance.below(upper)).unwrap_or(0)
    }

    fn count(&self, bump: impl FnOnce(&mut PartCounts)) {
        bump(&mut self.counts.lock().unwrap_or_else(PoisonError::into_inner));
    }
}

/// The parts the S3 store has started and not yet sent, and when each lands (module doc).
#[derive(Clone)]
pub struct Landing {
    lands: Arc<Lands>,
    lanes: Arc<Manual>,
    wire: Wire,
}

impl std::fmt::Debug for Landing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Landing")
            .field("started", &self.started())
            .finish_non_exhaustive()
    }
}

impl Landing {
    fn new(wire: &Wire, seed: u64) -> Self {
        let lands = Arc::new(Lands {
            chance: SeededEntropy::new(seed),
            by_hand: AtomicBool::new(false),
            landed_none: AtomicBool::new(false),
            counts: Mutex::new(PartCounts::default()),
        });
        let pick = Arc::clone(&lands);
        // A store that waits for a part lands them one at a time, in the order the seed
        // draws; oldest first for a scenario that lands its own.
        let lanes = Manual::new(move |started| {
            pick.count(|counts| counts.landed = counts.landed.saturating_add(1));
            if pick.by_hand.load(Ordering::SeqCst) {
                0
            } else {
                pick.one_of(started)
            }
        });
        Self {
            lands,
            lanes: Arc::new(lanes),
            wire: Arc::clone(wire),
        }
    }

    /// How many parts are started and not sent.
    #[must_use]
    pub fn started(&self) -> usize {
        self.lanes.started()
    }

    /// What became of the run's parts so far.
    #[must_use]
    pub fn counts(&self) -> PartCounts {
        *self
            .lands
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// From now on a question lands nothing (`true`), or the seed's draw again.
    pub fn by_hand(&self, on: bool) {
        self.lands.by_hand.store(on, Ordering::SeqCst);
    }

    /// Lands the part at `at`, counted from zero in the order they were started; `false`
    /// when there is none. Whether its request met a provider fault is counted.
    pub fn land(&self, at: usize) -> bool {
        let fired = self.wire.fired().len();
        if !self.lanes.land(at) {
            return false;
        }
        let faulted = self.wire.fired().len() > fired;
        self.lands.count(|counts| {
            counts.landed = counts.landed.saturating_add(1);
            counts.out_of_order = counts.out_of_order.saturating_add(u64::from(at > 0));
            counts.faulted = counts.faulted.saturating_add(u64::from(faulted));
        });
        true
    }

    /// The engine asks an upload about its parts: the seed lands none, some or all of the
    /// parts started (module doc).
    fn at_question(&self) {
        let started = self.started();
        if started == 0 || self.lands.by_hand.load(Ordering::SeqCst) {
            return;
        }
        let mut most = self.lands.one_of(started.saturating_add(1));
        if most == 0 && self.lands.landed_none.swap(true, Ordering::SeqCst) {
            most = 1;
        }
        if most > 0 {
            self.lands.landed_none.store(false, Ordering::SeqCst);
        }
        for _ in 0..most {
            let fired = self.wire.fired().len();
            let left = self.started();
            if left == 0 || !self.land(self.lands.one_of(left)) {
                break;
            }
            // One provider fault for one question (module doc).
            if self.wire.fired().len() > fired {
                break;
            }
        }
    }

    /// What the upload answered the question with.
    fn answered(&self, flight: Result<Flight, &StoreError>) {
        let left = self.started() > 0;
        self.lands.count(|counts| {
            counts.left_started = counts.left_started.saturating_add(u64::from(left));
            counts.full = counts
                .full
                .saturating_add(u64::from(flight == Ok(Flight::Full)));
        });
    }

    /// The process ends: each part still started reached the bucket or was never sent, by
    /// the seed, whatever a scenario held by hand.
    pub fn at_exit(&self) {
        while self.started() > 0 {
            if self.lands.chance.below(2) == 0 {
                self.lanes.land(0);
                self.lands.count(|counts| {
                    counts.landed_at_exit = counts.landed_at_exit.saturating_add(1)
                });
            } else {
                self.lanes.lose(0);
                self.lands
                    .count(|counts| counts.lost_at_exit = counts.lost_at_exit.saturating_add(1));
            }
        }
        self.by_hand(false);
    }
}

/// The world's clock, shared with the bucket (its last-modified times) and the store (its
/// signatures and deadlines).
#[derive(Debug, Clone)]
struct SharedClock(Arc<FakeClock>);

impl Clock for SharedClock {
    fn now(&self) -> Timestamp {
        self.0.now()
    }
}

/// The key prefix the replica lives under in the bucket.
const PREFIX: &str = "db";

/// The bucket, its address and the prefix, as the store is configured; with `parts`, their
/// size and how many go at once.
fn config(parts: Option<Parts>) -> Option<S3Config> {
    let config = S3Config::new(
        Endpoint::new("http://sim.invalid:9000").ok()?,
        BucketName::new("sim").ok()?,
        Prefix::new(PREFIX).ok()?,
    );
    Some(match parts {
        Some(parts) => config
            .with_part_size(PartSize::below_the_minimum(parts.part))
            .with_concurrency(parts.lanes),
        None => config,
    })
}

/// The S3 store, and its parts' lanes when the run sends in parts.
#[derive(Debug)]
pub struct S3Remote {
    store: S3Store<WireTransport>,
    landing: Option<Landing>,
    wire: Wire,
    /// The most multipart uploads the bucket held open right after a sweep that succeeded.
    left_by_sweep: AtomicU64,
}

impl S3Remote {
    /// The parts started and when they land, when the run sends in parts.
    #[must_use]
    pub const fn landing(&self) -> Option<&Landing> {
        self.landing.as_ref()
    }

    /// The process is ending (module doc): each part it has started lands or is lost. Call
    /// while the engine still holds its uploads: one already dropped sends nothing more.
    pub fn process_ending(&self) {
        if let Some(landing) = &self.landing {
            landing.at_exit();
        }
    }

    /// The process is gone, and with it every upload: the ids it meant to abort are
    /// forgotten, so the next boot's sweep has only the bucket's listing.
    pub fn process_ended(&self) {
        self.store.forget_orphans();
    }

    /// The most multipart uploads the bucket held open right after a sweep of abandoned
    /// uploads that reported success. A sweep runs at boot, before the process starts an
    /// upload of its own, and aborts every upload open under the prefix: none is left.
    #[must_use]
    pub fn left_by_sweep(&self) -> u64 {
        self.left_by_sweep.load(Ordering::SeqCst)
    }

    async fn sweep(&self) -> Result<u64, StoreError> {
        let swept = self.store.remove_scratch().await?;
        let open = u64::try_from(self.wire.inner().open_uploads()).unwrap_or(u64::MAX);
        self.left_by_sweep.fetch_max(open, Ordering::SeqCst);
        Ok(swept)
    }
}

/// The replica store the engine talks to, under the world's wrappers.
#[derive(Debug, Clone)]
pub enum RemoteBackend {
    /// The simulator's in-memory store.
    Mem(MemStore),
    /// The S3 store over the in-memory bucket behind the fault layer.
    S3(Arc<S3Remote>),
}

/// An upload through [`RemoteBackend`].
#[derive(Debug)]
pub enum RemoteUpload {
    /// Through `MemStore`.
    Mem(<MemStore as ObjectStore>::Upload),
    /// Through the S3 store, with its parts' lanes when the run sends in parts. Boxed: the
    /// S3 upload is several times the size of the other.
    S3(Box<(S3Upload<WireTransport>, Option<Landing>)>),
}

/// The replica as the harness reads and edits it (module doc).
#[derive(Debug, Clone)]
pub enum Bucket {
    /// The same `MemStore` the engine's store is.
    Mem(MemStore),
    /// The objects of the in-memory bucket, read without the S3 client.
    S3(Wire),
}

/// The S3 backend: the engine's store over a fresh bucket, and the harness's view of the same
/// bucket. With `parts` (and the seed their landings are drawn from) the store sends in parts
/// (module doc). `None` only if the fixed configuration were refused.
#[must_use]
pub fn s3(clock: &Arc<FakeClock>, parts: Option<(Parts, u64)>) -> Option<(RemoteBackend, Bucket)> {
    let config = config(parts.map(|(parts, _)| parts))?;
    let fake = FakeS3::new(&config, SharedClock(Arc::clone(clock)));
    if parts.is_some() {
        fake.accept_small_parts();
    }
    let wire: Wire = Arc::new(FaultTransport::new(fake));
    let landing = parts.map(|(_, seed)| Landing::new(&wire, seed));
    let credentials = S3Credentials::new("SIMKEY".to_owned(), SecretString::new("sim".to_owned()));
    let transport = WireTransport {
        wire: Arc::clone(&wire),
        lanes: landing.as_ref().map(|landing| Arc::clone(&landing.lanes)),
    };
    let store = S3Store::with_transport(
        config,
        credentials,
        transport,
        SharedClock(Arc::clone(clock)),
    )
    .ok()?;
    let remote = S3Remote {
        store,
        landing,
        wire: Arc::clone(&wire),
        left_by_sweep: AtomicU64::new(0),
    };
    Some((RemoteBackend::S3(Arc::new(remote)), Bucket::S3(wire)))
}

/// The store's fault layer's form of a drawn provider fault; `None` for a bucket policy
/// ([`HttpFault::DenyDeletes`]), which the bucket itself applies.
#[must_use]
pub fn provider_fault(fault: HttpFault) -> Option<Fault> {
    let status = |on, status, code| Fault::Status {
        on,
        status,
        code: Some(code),
        retry_after: None,
    };
    Some(match fault {
        HttpFault::SlowDown(on) => status(shape(on), 503, "SlowDown"),
        HttpFault::TooManyRequests(on) => Fault::Status {
            on: shape(on),
            status: 429,
            code: Some("TooManyRequests"),
            retry_after: Some("1"),
        },
        HttpFault::Internal(on) => status(shape(on), 500, "InternalError"),
        HttpFault::RequestTimeout(on) => status(shape(on), 408, "RequestTimeout"),
        HttpFault::AfterPublish(on) => Fault::AfterPublish {
            on: shape(on),
            status: 500,
        },
        HttpFault::TruncateBody(on) => Fault::TruncateBody {
            on: shape(on),
            keep: 16,
        },
        HttpFault::WrongEtag(on) => Fault::WrongEtag { on: shape(on) },
        HttpFault::ConnectionReset(on) => Fault::Transport {
            on: shape(on),
            err: TransportError::ConnectionReset,
        },
        HttpFault::Denied(on, code) => {
            let (status, code) = if matches!(on, HttpShape::Head) {
                head_answer(code)
            } else {
                denial_answer(code)
            };
            Fault::Status {
                on: shape(on),
                status,
                code,
                retry_after: None,
            }
        }
        HttpFault::DenyDeletes => return None,
    })
}

/// The status and S3 error code a provider answers a refusal with.
const fn denial_answer(code: DenialCode) -> (u16, Option<&'static str>) {
    match code {
        DenialCode::AccessDenied => (403, Some("AccessDenied")),
        DenialCode::AllAccessDisabled => (403, Some("AllAccessDisabled")),
        DenialCode::InvalidAccessKeyId => (403, Some("InvalidAccessKeyId")),
        DenialCode::SignatureDoesNotMatch => (403, Some("SignatureDoesNotMatch")),
        DenialCode::ExpiredToken => (400, Some("ExpiredToken")),
        DenialCode::Unauthorized => (401, None),
        DenialCode::RequestTimeTooSkewed => (403, Some("RequestTimeTooSkewed")),
        DenialCode::NoSuchBucket => (404, Some("NoSuchBucket")),
        DenialCode::KmsDisabled => (400, Some("KMS.DisabledException")),
        DenialCode::KmsNotFound => (400, Some("KMS.NotFoundException")),
    }
}

/// A HEAD's answer has no body, so a refusal shows only its status: KMS is not consulted
/// for metadata, and a missing bucket reads as a missing object, so both are drawn as the
/// bare 403 a policy refusal gives.
const fn head_answer(code: DenialCode) -> (u16, Option<&'static str>) {
    match denial_answer(code) {
        (401, _) => (401, None),
        _ => (403, None),
    }
}

/// The refusal a fired fault answered with, when it was one.
#[must_use]
pub fn denial_of(fault: &Fault) -> Option<lsr_core::Denied> {
    let Fault::Status { status, code, .. } = fault else {
        return None;
    };
    if (*status, *code) == (403, None) {
        return Some(lsr_core::Denied::Permission);
    }
    ALL_DENIALS
        .iter()
        .find(|denial| denial_answer(**denial) == (*status, *code))
        .map(|denial| expected_denial(*denial))
}

const ALL_DENIALS: [DenialCode; 10] = [
    DenialCode::AccessDenied,
    DenialCode::AllAccessDisabled,
    DenialCode::InvalidAccessKeyId,
    DenialCode::SignatureDoesNotMatch,
    DenialCode::ExpiredToken,
    DenialCode::Unauthorized,
    DenialCode::RequestTimeTooSkewed,
    DenialCode::NoSuchBucket,
    DenialCode::KmsDisabled,
    DenialCode::KmsNotFound,
];

/// What an operator must fix for each refusal, as the engine must report it: the simulator's
/// own reading of AWS's codes, kept apart from the store's classifier so a misreading there is
/// not agreed with. A KMS key that is disabled or gone is the bucket owner's to fix, as a
/// policy refusal is.
#[must_use]
pub const fn expected_denial(code: DenialCode) -> lsr_core::Denied {
    use lsr_core::Denied;
    match code {
        DenialCode::AccessDenied
        | DenialCode::AllAccessDisabled
        | DenialCode::KmsDisabled
        | DenialCode::KmsNotFound => Denied::Permission,
        DenialCode::InvalidAccessKeyId
        | DenialCode::SignatureDoesNotMatch
        | DenialCode::ExpiredToken
        | DenialCode::Unauthorized => Denied::Credentials,
        DenialCode::RequestTimeTooSkewed => Denied::ClockSkew,
        DenialCode::NoSuchBucket => Denied::BucketMissing,
    }
}

const fn shape(on: HttpShape) -> Shape {
    match on {
        HttpShape::List => Shape::List,
        HttpShape::Head => Shape::Head,
        HttpShape::Get => Shape::Get,
        HttpShape::Put => Shape::Put,
        HttpShape::DeleteObjects => Shape::DeleteObjects,
        HttpShape::ListUploads => Shape::ListUploads,
        HttpShape::CreateMultipart => Shape::CreateMultipart,
        HttpShape::UploadPart => Shape::UploadPart,
        HttpShape::Complete => Shape::Complete,
        HttpShape::Abort => Shape::Abort,
    }
}

/// The replica key a bucket key names, when it is one.
fn replica_key(object: &str) -> Option<FileKey> {
    let rest = object.strip_prefix(PREFIX)?.strip_prefix('/')?;
    let (level, name) = rest.split_once('/')?;
    if level.len() != 4 {
        return None;
    }
    let level = Level::new(u8::from_str_radix(level, 16).ok()?)?;
    let range = parse_file_name(name).ok()?;
    Some(FileKey::new(level, range))
}

/// The bucket key of a replica key.
fn object_key(key: &FileKey) -> String {
    format!("{PREFIX}/{:04x}/{}", key.level().get(), key.file_name())
}

/// The metadata name the replica's writers store a created-at time under.
const CREATED_AT: &str = "x-amz-meta-litestream-timestamp";

/// A replica object as the bucket stores it: its key, length and times. An object whose
/// created-at metadata is missing or unreadable reports its last-modified time as a fallback,
/// as a store must.
struct Stored {
    key: FileKey,
    size: u64,
    created_at: CreatedAt,
}

impl Stored {
    fn of(fake: &FakeS3, object: &str) -> Option<Self> {
        let key = replica_key(object)?;
        let size = u64::try_from(fake.size_of(object)?).ok()?;
        let modified = fake.last_modified_of(object)?;
        let created = fake.meta_of(object).and_then(|meta| {
            meta.iter()
                .find(|(name, _)| name == CREATED_AT)
                .and_then(|(_, value)| Timestamp::parse_rfc3339(value).ok())
        });
        Some(Self {
            key,
            size,
            created_at: created.map_or(CreatedAt::fallback(modified), CreatedAt::mtime),
        })
    }

    const fn info(&self) -> FileInfo {
        FileInfo {
            key: self.key,
            size: self.size,
            listed: lsr_core::ListedTime::Created(self.created_at),
        }
    }
}

/// A level of the bucket, as `MemStore` lists one: the replica files in it, and the names in
/// it that are not replica keys.
fn list_bucket(fake: &FakeS3, level: Level, after: Option<TxidRange>, limit: PageLimit) -> Listing {
    let prefix = format!("{PREFIX}/{:04x}/", level.get());
    let mut files = Vec::new();
    let mut unrecognized = Vec::new();
    for object in fake.keys() {
        let Some(name) = object.strip_prefix(&prefix) else {
            continue;
        };
        match Stored::of(fake, &object) {
            Some(stored) => files.push(stored.info()),
            None => unrecognized.push(name.to_owned()),
        }
    }
    lsr_core::build_listing(files, &unrecognized, after, limit)
}

/// Bytes `[offset, offset + len)` of the object under `key`, as `MemStore` reads them.
fn read_bucket(
    fake: &FakeS3,
    key: &FileKey,
    offset: u64,
    len: ReadLen,
) -> Result<Vec<u8>, StoreError> {
    let body = fake
        .body_of(&object_key(key))
        .ok_or(StoreError::NotFound { key: *key })?;
    let start = usize::try_from(offset).unwrap_or(usize::MAX);
    let want = usize::try_from(len.get()).unwrap_or(usize::MAX);
    let tail = body.get(start..).unwrap_or_default();
    Ok(tail.get(..want).unwrap_or(tail).to_vec())
}

/// The created-at time of `file`, as `MemStore` answers: gone is not found, another length is
/// a transient failure (the object changed since the listing).
fn created_in_bucket(fake: &FakeS3, file: &FileInfo) -> Result<CreatedAt, StoreError> {
    let stored =
        Stored::of(fake, &object_key(&file.key)).ok_or(StoreError::NotFound { key: file.key })?;
    if stored.size != file.size {
        return Err(StoreError::Transient {
            op: lsr_core::StoreOp::Read,
            kind: std::io::ErrorKind::InvalidData,
        });
    }
    Ok(stored.created_at)
}

impl Bucket {
    /// The S3 bucket's fault layer, when the replica is on S3.
    #[must_use]
    pub fn wire(&self) -> Option<&Wire> {
        match self {
            Self::Mem(_) => None,
            Self::S3(wire) => Some(wire),
        }
    }

    /// From now on the bucket has no conditional writes, as a provider that answers
    /// `If-None-Match` with `NotImplemented` (`docs/decisions/D079.md`): `MemStore` refuses every
    /// create-only commit, and the S3 bucket answers every conditional write with 501.
    pub fn refuse_create_only(&self) {
        match self {
            Self::Mem(store) => store.refuse_create_only(),
            Self::S3(wire) => wire.inner().refuse_conditional_writes(),
        }
    }

    /// What the bucket holds at a key, for the recorder (`crate::store::Landed`): the content
    /// digest of the object there, read from the bucket's own store.
    #[must_use]
    pub fn landed(&self) -> crate::store::Landed {
        let this = self.clone();
        Arc::new(move |key: &FileKey| {
            match &this {
                Self::Mem(store) => block_on(read_whole(store, key)),
                Self::S3(wire) => wire.inner().body_of(&object_key(key)),
            }
            .map(|bytes| crate::store::content_digest(&bytes))
        })
    }

    /// Every replica file the bucket holds, ascending by key, read from the bucket's own store
    /// (`crate::oracle::replica_files`): never through a listing's paging or merge.
    #[must_use]
    pub fn stored(&self) -> Vec<FileInfo> {
        match self {
            Self::Mem(store) => store.stored(),
            Self::S3(wire) => {
                let fake = wire.inner();
                let mut files: Vec<FileInfo> = fake
                    .keys()
                    .iter()
                    .filter_map(|object| Stored::of(fake, object).map(|stored| stored.info()))
                    .collect();
                files.sort_by_key(|info| info.key);
                files
            }
        }
    }

    /// Moves what a listing reports as `key`'s last-modified time.
    pub fn set_last_modified(&self, key: &FileKey, at: Timestamp) {
        match self {
            Self::Mem(store) => store.set_last_modified(key, at),
            Self::S3(wire) => wire.inner().set_last_modified(&object_key(key), at),
        }
    }

    /// A bucket policy refuses deleting each replica object the bucket holds now
    /// ([`HttpFault::DenyDeletes`]); nothing with `MemStore`.
    pub fn deny_deletes(&self) {
        if let Self::S3(wire) = self {
            let fake = wire.inner();
            for object in fake.keys() {
                if replica_key(&object).is_some() {
                    fake.deny_delete(&object);
                }
            }
        }
    }

    /// Lifts every delete refusal: the policy is gone.
    pub fn allow_deletes(&self) {
        if let Self::S3(wire) = self {
            wire.inner().allow_deletes();
        }
    }

    /// How many keys a DeleteObjects was refused for so far.
    #[must_use]
    pub fn refused_deletes(&self) -> u64 {
        match self {
            Self::Mem(_) => 0,
            Self::S3(wire) => wire.inner().refused_deletes(),
        }
    }

    /// Forgets the created-at time stored with `key`, as a copy that drops metadata does.
    pub fn drop_created_at(&self, key: &FileKey) {
        match self {
            Self::Mem(store) => store.drop_created_at(key),
            Self::S3(wire) => wire.inner().drop_meta(&object_key(key)),
        }
    }
}

/// The whole object under `key` in `store`, `None` when it holds none.
async fn read_whole(store: &MemStore, key: &FileKey) -> Option<Vec<u8>> {
    let len = ReadLen::new(ReadLen::MAX_RAW)?;
    store.read(key, 0, len).await.ok()
}

/// An upload through [`Bucket`]: with S3, held aside and stored whole at commit.
#[derive(Debug)]
pub enum BucketUpload {
    /// Through `MemStore`.
    Mem(<MemStore as ObjectStore>::Upload),
    /// Into the bucket directly.
    S3 {
        /// The bucket.
        wire: Wire,
        /// The replica key.
        key: FileKey,
        /// The created-at time stored with it.
        created_at: Timestamp,
        /// Only into a free key.
        new: bool,
        /// The bytes so far.
        buf: Vec<u8>,
    },
}

impl ObjectStore for Bucket {
    type Upload = BucketUpload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        match self {
            Self::Mem(store) => store.list(level, after, limit).await,
            Self::S3(wire) => Ok(list_bucket(wire.inner(), level, after, limit)),
        }
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Mem(store) => store.read(key, offset, len).await,
            Self::S3(wire) => read_bucket(wire.inner(), key, offset, len),
        }
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        match self {
            Self::Mem(store) => store.created_at(file).await,
            Self::S3(wire) => created_in_bucket(wire.inner(), file),
        }
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        match self {
            Self::Mem(store) => store.read_with_created_at(file, len).await,
            Self::S3(wire) => Ok((
                read_bucket(wire.inner(), &file.key, 0, len)?,
                created_in_bucket(wire.inner(), file)?,
            )),
        }
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<BucketUpload, StoreError> {
        match self {
            Self::Mem(store) => store
                .begin_sized_upload(key, created_at, mode, len)
                .await
                .map(BucketUpload::Mem),
            Self::S3(wire) => Ok(BucketUpload::S3 {
                wire: Arc::clone(wire),
                key: *key,
                created_at,
                new: mode == WriteMode::New,
                buf: Vec::new(),
            }),
        }
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        match self {
            Self::Mem(store) => store.delete(keys).await,
            Self::S3(wire) => {
                for key in keys {
                    wire.inner().delete_raw(&object_key(key));
                }
                Ok(())
            }
        }
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        match self {
            Self::Mem(store) => store.remove_scratch().await,
            Self::S3(_) => Ok(0),
        }
    }
}

impl Upload for BucketUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        match self {
            Self::Mem(up) => up.write(chunk).await,
            Self::S3 { buf, .. } => {
                buf.extend_from_slice(chunk);
                Ok(())
            }
        }
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let (wire, key, created_at, new, buf) = match self {
            Self::Mem(up) => return up.commit().await,
            Self::S3 {
                wire,
                key,
                created_at,
                new,
                buf,
            } => (wire, key, created_at, new, buf),
        };
        let object = object_key(&key);
        if new && wire.inner().body_of(&object).is_some() {
            return Err(StoreError::AlreadyExists { key });
        }
        let size = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        let stamp = created_at.to_string();
        wire.inner()
            .put_raw(&object, buf, &[(CREATED_AT, stamp.as_str())]);
        Ok(FileInfo {
            key,
            size,
            listed: lsr_core::ListedTime::Created(CreatedAt::metadata(created_at)),
        })
    }

    async fn abort(self) -> Result<(), StoreError> {
        match self {
            Self::Mem(up) => up.abort().await,
            Self::S3 { .. } => Ok(()),
        }
    }
}

impl ObjectStore for RemoteBackend {
    type Upload = RemoteUpload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        match self {
            Self::Mem(s) => s.list(level, after, limit).await,
            Self::S3(s) => s.store.list(level, after, limit).await,
        }
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Mem(s) => s.read(key, offset, len).await,
            Self::S3(s) => s.store.read(key, offset, len).await,
        }
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        match self {
            Self::Mem(s) => s.created_at(file).await,
            Self::S3(s) => s.store.created_at(file).await,
        }
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        match self {
            Self::Mem(s) => s.read_with_created_at(file, len).await,
            Self::S3(s) => s.store.read_with_created_at(file, len).await,
        }
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<RemoteUpload, StoreError> {
        match self {
            Self::Mem(s) => s
                .begin_sized_upload(key, created_at, mode, len)
                .await
                .map(RemoteUpload::Mem),
            Self::S3(s) => s
                .store
                .begin_sized_upload(key, created_at, mode, len)
                .await
                .map(|upload| RemoteUpload::S3(Box::new((upload, s.landing.clone())))),
        }
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        match self {
            Self::Mem(s) => s.delete(keys).await,
            Self::S3(s) => s.store.delete(keys).await,
        }
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        match self {
            Self::Mem(s) => s.remove_scratch().await,
            Self::S3(s) => s.sweep().await,
        }
    }

    fn batch_width(&self, kind: BatchKind) -> std::num::NonZeroUsize {
        match self {
            Self::Mem(s) => s.batch_width(kind),
            Self::S3(s) => s.store.batch_width(kind),
        }
    }

    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        match self {
            Self::Mem(s) => s.created_at_many(files).await,
            Self::S3(s) => s.store.created_at_many(files).await,
        }
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        match self {
            Self::Mem(s) => s.list_many(pages, limit).await,
            Self::S3(s) => s.store.list_many(pages, limit).await,
        }
    }
}

impl Upload for RemoteUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.write(chunk).await,
            Self::S3(u) => u.0.write(chunk).await,
        }
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.write_from(chunk, again).await,
            Self::S3(u) => u.0.write_from(chunk, again).await,
        }
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        match self {
            Self::Mem(u) => u.commit().await,
            Self::S3(u) => u.0.commit().await,
        }
    }

    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        match self {
            Self::Mem(u) => u.flight(rest).await,
            Self::S3(u) => {
                let (upload, landing) = &mut **u;
                if let Some(landing) = landing {
                    landing.at_question();
                }
                let flight = upload.flight(rest).await;
                if let Some(landing) = landing {
                    landing.answered(flight.as_ref().copied());
                }
                flight
            }
        }
    }

    async fn abort(self) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.abort().await,
            Self::S3(u) => u.0.abort().await,
        }
    }
}
