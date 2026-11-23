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

use std::sync::Arc;

use lsr_core::conformance::block_on;
use lsr_core::{
    BatchKind, Clock, CreatedAt, FakeClock, FileInfo, FileKey, Level, ListAt, Listing, MemStore,
    ObjectStore, PageLimit, ReadAt, ReadLen, StoreError, Upload, WriteMode, parse_file_name,
};
use lsr_ltx::{Timestamp, TxidRange};
use lsr_store_s3::{
    BucketName, Endpoint, FakeS3, Fault, FaultTransport, HttpRequest, HttpResponse, Prefix,
    S3Config, S3Credentials, S3Store, S3Upload, SecretString, Shape, Transport, TransportError,
};

use crate::generate::{DenialCode, HttpFault, HttpShape};

/// The bucket's fault layer, shared by the store that owns a transport and the harness that
/// scripts faults and reads the bucket.
pub type Wire = Arc<FaultTransport<FakeS3>>;

/// The store's transport: the shared [`Wire`].
#[derive(Debug, Clone)]
pub struct WireTransport(Wire);

impl Transport for WireTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.0.send(request)
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

/// The bucket, its address and the prefix, as the store is configured.
fn config() -> Option<S3Config> {
    Some(S3Config::new(
        Endpoint::new("http://sim.invalid:9000").ok()?,
        BucketName::new("sim").ok()?,
        Prefix::new(PREFIX).ok()?,
    ))
}

/// The replica store the engine talks to, under the world's wrappers.
#[derive(Debug, Clone)]
pub enum RemoteBackend {
    /// The simulator's in-memory store.
    Mem(MemStore),
    /// The S3 store over the in-memory bucket behind the fault layer.
    S3(Arc<S3Store<WireTransport>>),
}

/// An upload through [`RemoteBackend`].
#[derive(Debug)]
pub enum RemoteUpload {
    /// Through `MemStore`.
    Mem(<MemStore as ObjectStore>::Upload),
    /// Through the S3 store.
    S3(S3Upload<WireTransport>),
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
/// bucket. `None` only if the fixed configuration were refused.
#[must_use]
pub fn s3(clock: &Arc<FakeClock>) -> Option<(RemoteBackend, Bucket)> {
    let config = config()?;
    let fake = FakeS3::new(&config, SharedClock(Arc::clone(clock)));
    let wire: Wire = Arc::new(FaultTransport::new(fake));
    let credentials = S3Credentials::new("SIMKEY".to_owned(), SecretString::new("sim".to_owned()));
    let store = S3Store::with_transport(
        config,
        credentials,
        WireTransport(Arc::clone(&wire)),
        SharedClock(Arc::clone(clock)),
    )
    .ok()?;
    Some((RemoteBackend::S3(Arc::new(store)), Bucket::S3(wire)))
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
    Ok(body
        .get(start..)
        .unwrap_or_default()
        .iter()
        .take(want)
        .copied()
        .collect())
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<BucketUpload, StoreError> {
        match self {
            Self::Mem(store) => store
                .begin_upload(key, created_at, mode)
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
            Self::S3(s) => s.list(level, after, limit).await,
        }
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Mem(s) => s.read(key, offset, len).await,
            Self::S3(s) => s.read(key, offset, len).await,
        }
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        match self {
            Self::Mem(s) => s.created_at(file).await,
            Self::S3(s) => s.created_at(file).await,
        }
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        match self {
            Self::Mem(s) => s.read_with_created_at(file, len).await,
            Self::S3(s) => s.read_with_created_at(file, len).await,
        }
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<RemoteUpload, StoreError> {
        match self {
            Self::Mem(s) => s
                .begin_upload(key, created_at, mode)
                .await
                .map(RemoteUpload::Mem),
            Self::S3(s) => s
                .begin_upload(key, created_at, mode)
                .await
                .map(RemoteUpload::S3),
        }
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        match self {
            Self::Mem(s) => s.delete(keys).await,
            Self::S3(s) => s.delete(keys).await,
        }
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        match self {
            Self::Mem(s) => s.remove_scratch().await,
            Self::S3(s) => s.remove_scratch().await,
        }
    }

    fn batch_width(&self, kind: BatchKind) -> std::num::NonZeroUsize {
        match self {
            Self::Mem(s) => s.batch_width(kind),
            Self::S3(s) => s.batch_width(kind),
        }
    }

    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        match self {
            Self::Mem(s) => s.read_many(reads).await,
            Self::S3(s) => s.read_many(reads).await,
        }
    }

    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        match self {
            Self::Mem(s) => s.created_at_many(files).await,
            Self::S3(s) => s.created_at_many(files).await,
        }
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        match self {
            Self::Mem(s) => s.list_many(pages, limit).await,
            Self::S3(s) => s.list_many(pages, limit).await,
        }
    }
}

impl Upload for RemoteUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.write(chunk).await,
            Self::S3(u) => u.write(chunk).await,
        }
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        match self {
            Self::Mem(u) => u.commit().await,
            Self::S3(u) => u.commit().await,
        }
    }

    async fn abort(self) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.abort().await,
            Self::S3(u) => u.abort().await,
        }
    }
}
