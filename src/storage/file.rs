//! [`FileStorage`]: the real, disk-backed [`Storage`] implementation.
//!
//! Layout inside the data directory:
//!
//! - `log` — an 8-byte little-endian start-index header (the global index of
//!   the first record, `snapshot_last_index + 1`) followed by records. Each
//!   record is `[u32 LE payload length][u32 LE CRC32C of payload][payload]`; a
//!   payload is `[u64 LE term][u8 kind]` then, for a command, `[u32 LE length]
//!   [command bytes]`, or for a configuration, `[u32 LE voter count][u64 LE
//!   voter id]*count`. Recovery reads records until one is short or fails its
//!   checksum and truncates the file there: a torn tail from a crash mid-append
//!   is expected, not treated as corruption.
//! - `meta.0`, `meta.1` — two independent copies of `{ currentTerm, votedFor }`,
//!   each `[u32 LE CRC32C][u64 LE term][u8 has_vote][u64 LE vote id]`. Keeping
//!   two lets recovery survive one being torn or corrupt. On load a copy that
//!   fails its checksum is ignored; if both parse but disagree the higher term
//!   wins and the vote is dropped (the conservative choice).
//!
//! Every write `fsync`s the file it touched, and the directory as well when a
//! file was newly created. A failed `fsync` is returned as the fatal
//! [`Error::Sync`].

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::{Error, PersistentState, Snapshot, SnapshotMeta, Storage, crc32c};
use crate::core::{ClusterConfig, LogEntry, LogEntryKind, LogIndex, NodeId, Term};

/// The on-disk metadata record is fixed size: crc(4) + term(8) + flag(1) +
/// vote(8).
const META_LEN: usize = 21;

/// The `log` file opens with an 8-byte little-endian `u64` giving the global
/// 1-based index of its first record: `1` for a never-compacted log,
/// `snapshot_last_index + 1` afterwards. It lets recovery place the records
/// without stamping an index on every one.
const LOG_HEADER_LEN: u64 = 8;

/// The snapshot file and the sibling it is staged in before an atomic rename.
const SNAPSHOT_FILE: &str = "snapshot";
const SNAPSHOT_TMP: &str = "snapshot.tmp";

/// Fixed-size head of the snapshot file: `crc(4)` + `last_included_index(8)` +
/// `last_included_term(8)` + `data_len(8)`.
const SNAPSHOT_HEAD_LEN: usize = 28;

/// Disk-backed durable storage for one Raft server.
///
/// Call [`FileStorage::load`] once before any `persist_*` call: it recovers
/// the log and, in doing so, learns the byte offset of every record and the
/// index the log starts at, which [`FileStorage::persist_log`] and
/// [`FileStorage::persist_snapshot`] need in order to truncate.
#[derive(Debug)]
pub struct FileStorage {
    dir: PathBuf,
    log_path: PathBuf,
    log: File,
    /// Byte offset (from the start of the file, [`LOG_HEADER_LEN`] header
    /// included) just past record `k`, for every record currently on disk.
    record_ends: Vec<u64>,
    /// Global 1-based index of the first on-disk record — the `log` file's
    /// header. Equals `snapshot_last_index + 1`.
    log_start: u64,
    loaded: bool,
}

impl FileStorage {
    /// Opens (creating if absent) the data directory and the log file.
    ///
    /// On Linux this also logs the data directory's filesystem type and warns
    /// if it is outside the tier exercised by CI (ext4, xfs); durability is
    /// expected to work elsewhere but is not tested there.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the directory or log file cannot be created or opened,
    /// or [`Error::Sync`] if the post-create directory `fsync` fails.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|source| Error::Io {
            path: dir.clone(),
            source,
        })?;

        warn_if_unusual_filesystem(&dir);

        let log_path = dir.join("log");
        let existed = log_path.exists();
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)
            .map_err(|source| Error::Io {
                path: log_path.clone(),
                source,
            })?;
        if !existed {
            // A fresh log starts at index 1; write and sync its header.
            log.write_all(&1u64.to_le_bytes())
                .map_err(|source| Error::Io {
                    path: log_path.clone(),
                    source,
                })?;
            log.sync_all().map_err(|source| Error::Sync {
                path: log_path.clone(),
                source,
            })?;
            sync_dir(&dir)?;
        }

        Ok(Self {
            dir,
            log_path,
            log,
            record_ends: Vec::new(),
            log_start: 1,
            loaded: false,
        })
    }

    fn meta_path(&self, which: u8) -> PathBuf {
        self.dir.join(format!("meta.{which}"))
    }

    /// Reads `log` record by record, stopping at the first torn or
    /// checksum-failing one, and truncates the file there.
    fn recover_log(&mut self) -> Result<Vec<LogEntry>, Error> {
        self.log
            .seek(SeekFrom::Start(0))
            .map_err(|source| self.io(source))?;

        // The 8-byte start-index header. A file too short to hold it is a
        // fresh or torn header: reset it to "starts at index 1".
        let mut header_bytes = [0u8; 8];
        match read_exact_or_eof(&mut self.log, &mut header_bytes)
            .map_err(|source| self.io(source))?
        {
            ReadOutcome::Full => {
                self.log_start = u64::from_le_bytes(header_bytes).max(1);
            }
            ReadOutcome::Eof | ReadOutcome::Short => {
                self.rewrite_log(1, &[])?;
                self.record_ends = Vec::new();
                return Ok(Vec::new());
            }
        }

        let mut entries = Vec::new();
        let mut ends = Vec::new();
        let mut offset: u64 = LOG_HEADER_LEN;

        loop {
            let mut header = [0u8; 8];
            match read_exact_or_eof(&mut self.log, &mut header).map_err(|source| self.io(source))? {
                // Clean end of file, or a torn header at the tail.
                ReadOutcome::Eof | ReadOutcome::Short => break,
                ReadOutcome::Full => {}
            }
            let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let want_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            let payload_len = payload_len as usize;

            let mut payload = vec![0u8; payload_len];
            match read_exact_or_eof(&mut self.log, &mut payload)
                .map_err(|source| self.io(source))?
            {
                ReadOutcome::Full => {}
                ReadOutcome::Eof | ReadOutcome::Short => break, // torn payload
            }
            if crc32c(&payload) != want_crc {
                break; // torn / corrupt tail
            }
            let Some(entry) = decode_entry(&payload) else {
                break; // internally inconsistent record: treat as torn
            };

            offset += 8 + payload_len as u64;
            entries.push(entry);
            ends.push(offset);
        }

        let file_len = self
            .log
            .seek(SeekFrom::End(0))
            .map_err(|source| self.io(source))?;
        if file_len != offset {
            // Drop the torn tail so the file is clean for the next append.
            self.log.set_len(offset).map_err(|source| self.io(source))?;
            self.log.sync_all().map_err(|source| Error::Sync {
                path: self.log_path.clone(),
                source,
            })?;
        }
        self.log
            .seek(SeekFrom::Start(offset))
            .map_err(|source| self.io(source))?;

        self.record_ends = ends;
        Ok(entries)
    }

    fn io(&self, source: std::io::Error) -> Error {
        Error::Io {
            path: self.log_path.clone(),
            source,
        }
    }

    /// Reads the `snapshot` file, or `Ok(None)` if there is none.
    ///
    /// The rename in [`FileStorage::persist_snapshot`] is atomic, so a file
    /// that exists is complete; a checksum failure is real corruption, not a
    /// torn tail, and surfaces as [`Error::Corrupt`].
    fn load_snapshot(&self) -> Result<Option<Snapshot>, Error> {
        let path = self.dir.join(SNAPSHOT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path, source }),
        };
        if bytes.len() < SNAPSHOT_HEAD_LEN {
            return Err(Error::Corrupt {
                detail: "snapshot file is shorter than its header".to_string(),
            });
        }
        let want_crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let body = &bytes[4..];
        if crc32c(body) != want_crc {
            return Err(Error::Corrupt {
                detail: "snapshot checksum mismatch".to_string(),
            });
        }
        let index = u64::from_le_bytes([
            body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
        ]);
        let term = u64::from_le_bytes([
            body[8], body[9], body[10], body[11], body[12], body[13], body[14], body[15],
        ]);
        let data_len = u64::from_le_bytes([
            body[16], body[17], body[18], body[19], body[20], body[21], body[22], body[23],
        ]);
        let data_len = usize::try_from(data_len).map_err(|_| Error::Corrupt {
            detail: "snapshot data length does not fit in memory".to_string(),
        })?;
        let data = body
            .get(24..)
            .and_then(|rest| rest.get(..data_len))
            .ok_or_else(|| Error::Corrupt {
                detail: "snapshot data is shorter than its length prefix".to_string(),
            })?;
        let after_data = body.get(24 + data_len..).ok_or_else(|| Error::Corrupt {
            detail: "snapshot is missing its configuration".to_string(),
        })?;
        let config = decode_voters(after_data).ok_or_else(|| Error::Corrupt {
            detail: "snapshot configuration is malformed".to_string(),
        })?;
        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_included_index: LogIndex::new(index),
                last_included_term: Term::new(term),
                config,
            },
            data: Bytes::copy_from_slice(data),
        }))
    }

    /// Drops the first `drop` on-disk records and rewrites the log with header
    /// `new_start`, `fsync`ing the result. Remaps `record_ends` for the
    /// survivors.
    fn drop_log_prefix(&mut self, drop: usize, new_start: u64) -> Result<(), Error> {
        let byte_start = if drop == 0 {
            LOG_HEADER_LEN
        } else {
            self.record_ends[drop - 1]
        };
        self.log
            .seek(SeekFrom::Start(byte_start))
            .map_err(|source| self.io(source))?;
        let mut surviving = Vec::new();
        self.log
            .read_to_end(&mut surviving)
            .map_err(|source| self.io(source))?;
        self.rewrite_log(new_start, &surviving)?;

        let shift = byte_start - LOG_HEADER_LEN;
        self.record_ends = self.record_ends.split_off(drop);
        for end in &mut self.record_ends {
            *end -= shift;
        }
        Ok(())
    }

    /// Truncates the log to nothing and writes `[start header][records]`,
    /// `fsync`ing before returning.
    fn rewrite_log(&mut self, start: u64, records: &[u8]) -> Result<(), Error> {
        self.log.set_len(0).map_err(|source| self.io(source))?;
        self.log
            .seek(SeekFrom::Start(0))
            .map_err(|source| self.io(source))?;
        self.log
            .write_all(&start.to_le_bytes())
            .map_err(|source| self.io(source))?;
        self.log
            .write_all(records)
            .map_err(|source| self.io(source))?;
        self.log.sync_all().map_err(|source| Error::Sync {
            path: self.log_path.clone(),
            source,
        })
    }
}

impl Storage for FileStorage {
    fn load(&mut self) -> Result<PersistentState, Error> {
        let (current_term, voted_for) = load_metadata(&self.meta_path(0), &self.meta_path(1))?;
        let snapshot = self.load_snapshot()?;
        let mut entries = self.recover_log()?;

        // If a crash landed between `persist_snapshot` making a snapshot
        // durable and it rewriting the log, the log still carries a few
        // records the snapshot now covers. Drop them so `entries[0]` sits at
        // `last_included_index + 1`.
        if let Some(snap) = &snapshot {
            let first_kept = snap.meta.last_included_index.get() + 1;
            if first_kept >= self.log_start {
                let drop = usize::try_from(first_kept - self.log_start)
                    .unwrap_or(usize::MAX)
                    .min(entries.len());
                if drop > 0 {
                    self.drop_log_prefix(drop, first_kept)?;
                    entries.drain(..drop);
                }
                self.log_start = first_kept;
            } else {
                return Err(Error::Corrupt {
                    detail: format!(
                        "log starts at index {} but the snapshot only covers through {}",
                        self.log_start, snap.meta.last_included_index,
                    ),
                });
            }
        }

        self.loaded = true;
        Ok(PersistentState {
            current_term,
            voted_for,
            snapshot,
            entries,
        })
    }

    fn persist_metadata(
        &mut self,
        current_term: Term,
        voted_for: Option<NodeId>,
    ) -> Result<(), Error> {
        let record = encode_metadata(current_term, voted_for);
        let paths = [self.meta_path(0), self.meta_path(1)];
        let mut created = false;
        for path in &paths {
            if !path.exists() {
                created = true;
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            file.write_all(&record).map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
            file.sync_all().map_err(|source| Error::Sync {
                path: path.clone(),
                source,
            })?;
        }
        if created {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    fn persist_log(&mut self, from_index: LogIndex, entries: &[LogEntry]) -> Result<(), Error> {
        debug_assert!(self.loaded, "persist_log called before load");
        // The core only ever passes `log_start <= from_index <= len + 1`.
        // Clamp defensively so a bug cannot truncate to a wild offset; the
        // `debug_assert` fails loudly in tests.
        let stored = self.record_ends.len();
        let wanted = from_index.get().saturating_sub(self.log_start);
        let keep = usize::try_from(wanted).unwrap_or(usize::MAX).min(stored);
        debug_assert!(
            keep as u64 == wanted,
            "persist_log from_index {} out of range (log starts at {}, {stored} stored)",
            from_index.get(),
            self.log_start,
        );

        let mut offset = if keep == 0 {
            LOG_HEADER_LEN
        } else {
            self.record_ends[keep - 1]
        };
        self.log.set_len(offset).map_err(|source| self.io(source))?;
        self.log
            .seek(SeekFrom::Start(offset))
            .map_err(|source| self.io(source))?;
        self.record_ends.truncate(keep);

        for entry in entries {
            let frame = frame_record(&encode_entry(entry)?)?;
            self.log
                .write_all(&frame)
                .map_err(|source| self.io(source))?;
            offset += frame.len() as u64;
            self.record_ends.push(offset);
        }

        self.log.sync_all().map_err(|source| Error::Sync {
            path: self.log_path.clone(),
            source,
        })
    }

    fn persist_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<(), Error> {
        debug_assert!(self.loaded, "persist_snapshot called before load");

        // 1. Write the snapshot durably: staging file, fsync, atomic rename,
        //    directory fsync. After this the snapshot at `last_included_index`
        //    has reached the disk.
        let tmp = self.dir.join(SNAPSHOT_TMP);
        let final_path = self.dir.join(SNAPSHOT_FILE);
        let record = encode_snapshot(&meta, data);
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|source| Error::Io {
                    path: tmp.clone(),
                    source,
                })?;
            file.write_all(&record).map_err(|source| Error::Io {
                path: tmp.clone(),
                source,
            })?;
            file.sync_all().map_err(|source| Error::Sync {
                path: tmp.clone(),
                source,
            })?;
        }
        std::fs::rename(&tmp, &final_path).map_err(|source| Error::Io {
            path: final_path.clone(),
            source,
        })?;
        sync_dir(&self.dir)?;

        // 2. Drop the now-covered prefix of the log and rewrite it with the
        //    new start index. A crash between steps 1 and 2 leaves a log with
        //    a covered head that `load` trims.
        let first_kept = meta.last_included_index.get() + 1;
        if first_kept > self.log_start {
            let drop = usize::try_from(first_kept - self.log_start)
                .unwrap_or(usize::MAX)
                .min(self.record_ends.len());
            self.drop_log_prefix(drop, first_kept)?;
            self.log_start = first_kept;
        }
        Ok(())
    }
}

/// Frames a payload: `[u32 LE len][u32 LE crc32c][payload]`.
///
/// # Errors
///
/// [`Error::RecordTooLarge`] if the payload does not fit the 4 GiB frame.
fn frame_record(payload: &[u8]) -> Result<Vec<u8>, Error> {
    let len = u32::try_from(payload.len()).map_err(|_| Error::RecordTooLarge {
        bytes: payload.len(),
    })?;
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32c(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// `[u64 LE term][u8 kind]` then, for a command (kind 0), `[u32 LE length]
/// [command bytes]`; for a configuration (kind 1), `[u32 LE voter count]
/// [u64 LE voter id]*count`.
fn encode_entry(entry: &LogEntry) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(13);
    out.extend_from_slice(&entry.term.get().to_le_bytes());
    match &entry.kind {
        LogEntryKind::Command(command) => {
            let len = u32::try_from(command.len()).map_err(|_| Error::RecordTooLarge {
                bytes: command.len(),
            })?;
            out.push(0);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(command);
        }
        LogEntryKind::Config(config) => {
            let count =
                u32::try_from(config.voters().len()).map_err(|_| Error::RecordTooLarge {
                    bytes: config.voters().len(),
                })?;
            out.push(1);
            out.extend_from_slice(&count.to_le_bytes());
            for id in config.voters() {
                out.extend_from_slice(&id.get().to_le_bytes());
            }
        }
    }
    Ok(out)
}

fn decode_entry(payload: &[u8]) -> Option<LogEntry> {
    let term = Term::new(u64::from_le_bytes(payload.get(0..8)?.try_into().ok()?));
    let kind = *payload.get(8)?;
    let rest = payload.get(9..)?;
    match kind {
        0 => {
            let len = u32::from_le_bytes(rest.get(0..4)?.try_into().ok()?) as usize;
            let command = rest.get(4..)?;
            if command.len() != len {
                return None;
            }
            Some(LogEntry::command(term, Bytes::copy_from_slice(command)))
        }
        1 => {
            let count = u32::from_le_bytes(rest.get(0..4)?.try_into().ok()?) as usize;
            let ids = rest.get(4..)?;
            if ids.len() != count * 8 {
                return None;
            }
            let mut voters = Vec::with_capacity(count);
            for k in 0..count {
                let start = k * 8;
                let id = u64::from_le_bytes(ids.get(start..start + 8)?.try_into().ok()?);
                voters.push(NodeId::new(id));
            }
            Some(LogEntry::config(term, ClusterConfig::new(voters)))
        }
        _ => None,
    }
}

fn encode_metadata(current_term: Term, voted_for: Option<NodeId>) -> [u8; META_LEN] {
    let mut payload = [0u8; META_LEN - 4];
    payload[0..8].copy_from_slice(&current_term.get().to_le_bytes());
    payload[8] = u8::from(voted_for.is_some());
    payload[9..17].copy_from_slice(&voted_for.map_or(0, NodeId::get).to_le_bytes());

    let mut record = [0u8; META_LEN];
    record[0..4].copy_from_slice(&crc32c(&payload).to_le_bytes());
    record[4..].copy_from_slice(&payload);
    record
}

/// `[u32 LE crc32c of the rest][u64 LE last_included_index][u64 LE
/// last_included_term][u64 LE data length][data][u32 LE voter count][u64 LE
/// voter id]*count`.
fn encode_snapshot(meta: &SnapshotMeta, data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(SNAPSHOT_HEAD_LEN - 4 + data.len());
    body.extend_from_slice(&meta.last_included_index.get().to_le_bytes());
    body.extend_from_slice(&meta.last_included_term.get().to_le_bytes());
    body.extend_from_slice(&(data.len() as u64).to_le_bytes());
    body.extend_from_slice(data);
    let count = u32::try_from(meta.config.voters().len()).unwrap_or(u32::MAX);
    body.extend_from_slice(&count.to_le_bytes());
    for id in meta.config.voters() {
        body.extend_from_slice(&id.get().to_le_bytes());
    }

    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&crc32c(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Decodes `[u32 LE count][u64 LE id]*count` into a [`ClusterConfig`].
fn decode_voters(bytes: &[u8]) -> Option<ClusterConfig> {
    let count = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
    let ids = bytes.get(4..)?;
    if ids.len() != count * 8 {
        return None;
    }
    let mut voters = Vec::with_capacity(count);
    for k in 0..count {
        let start = k * 8;
        voters.push(NodeId::new(u64::from_le_bytes(
            ids.get(start..start + 8)?.try_into().ok()?,
        )));
    }
    Some(ClusterConfig::new(voters))
}

/// One metadata copy as read from disk.
enum MetaRead {
    Missing,
    Corrupt,
    Value(Term, Option<NodeId>),
}

fn read_metadata(path: &Path) -> Result<MetaRead, Error> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MetaRead::Missing);
        }
        Err(source) => {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if bytes.len() != META_LEN {
        return Ok(MetaRead::Corrupt);
    }
    let want_crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let payload = &bytes[4..];
    if crc32c(payload) != want_crc {
        return Ok(MetaRead::Corrupt);
    }
    let term = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let has_vote = payload[8] != 0;
    let vote = u64::from_le_bytes([
        payload[9],
        payload[10],
        payload[11],
        payload[12],
        payload[13],
        payload[14],
        payload[15],
        payload[16],
    ]);
    Ok(MetaRead::Value(
        Term::new(term),
        has_vote.then(|| NodeId::new(vote)),
    ))
}

fn load_metadata(path0: &Path, path1: &Path) -> Result<(Term, Option<NodeId>), Error> {
    let a = read_metadata(path0)?;
    let b = read_metadata(path1)?;
    match (a, b) {
        (MetaRead::Missing, MetaRead::Missing) => Ok((Term::ZERO, None)),
        // A copy that fails its checksum is ignored in favour of the readable
        // one.
        (MetaRead::Value(term, vote), MetaRead::Missing | MetaRead::Corrupt)
        | (MetaRead::Missing | MetaRead::Corrupt, MetaRead::Value(term, vote)) => Ok((term, vote)),
        (MetaRead::Value(ta, va), MetaRead::Value(tb, vb)) => {
            // Both parse but disagree: higher term, and keep the vote only if
            // the two copies agree completely.
            let term = ta.max(tb);
            let vote = if ta == tb && va == vb { va } else { None };
            Ok((term, vote))
        }
        _ => Err(Error::Corrupt {
            detail: "no readable metadata copy (both missing or corrupt)".to_string(),
        }),
    }
}

fn sync_dir(dir: &Path) -> Result<(), Error> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| Error::Sync {
            path: dir.to_path_buf(),
            source,
        })
}

enum ReadOutcome {
    Full,
    Short,
    Eof,
}

/// Fills `buf`, distinguishing a clean end-of-file from a partial (torn) read.
fn read_exact_or_eof(file: &mut File, buf: &mut [u8]) -> std::io::Result<ReadOutcome> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..])? {
            0 if filled == 0 => return Ok(ReadOutcome::Eof),
            0 => return Ok(ReadOutcome::Short),
            n => filled += n,
        }
    }
    Ok(ReadOutcome::Full)
}

#[cfg(target_os = "linux")]
fn warn_if_unusual_filesystem(dir: &Path) {
    let Ok(canonical) = std::fs::canonicalize(dir) else {
        return;
    };
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return;
    };

    // Pick the mount whose mount point is the longest prefix of our directory.
    let mut best: Option<(usize, String, String)> = None;
    for line in mountinfo.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let mount_point = before.split_whitespace().nth(4).unwrap_or("");
        let fstype = after.split_whitespace().next().unwrap_or("");
        if canonical.starts_with(mount_point)
            && mount_point.len() > best.as_ref().map_or(0, |b| b.0)
        {
            best = Some((
                mount_point.len(),
                fstype.to_string(),
                mount_point.to_string(),
            ));
        }
    }

    if let Some((_, fstype, mount_point)) = best {
        let tier1 = matches!(fstype.as_str(), "ext4" | "xfs");
        if tier1 {
            log::info!(
                "raft data dir {} is on {fstype} ({mount_point})",
                dir.display()
            );
        } else {
            log::warn!(
                "raft data dir {} is on {fstype} ({mount_point}), which is outside the \
                 CI-exercised tier (ext4, xfs); durability is expected to work but is not tested",
                dir.display(),
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn warn_if_unusual_filesystem(_dir: &Path) {}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::Path;

    use bytes::Bytes;
    use tempfile::{TempDir, tempdir};

    use super::{FileStorage, encode_metadata, encode_snapshot};
    use crate::core::{ClusterConfig, LogEntry, LogIndex, NodeId, Term};
    use crate::storage::{Error, PersistentState, SnapshotMeta, Storage};

    fn snap_meta(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex::new(index),
            last_included_term: Term::new(term),
            config: ClusterConfig::new([NodeId::new(1), NodeId::new(2), NodeId::new(3)]),
        }
    }

    #[track_caller]
    fn ok<T>(result: Result<T, Error>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => unreachable!("expected Ok, got {err}"),
        }
    }

    fn entry(term: u64, cmd: &'static [u8]) -> LogEntry {
        LogEntry::command(Term::new(term), Bytes::from_static(cmd))
    }

    fn scratch() -> TempDir {
        ok(tempdir().map_err(|source| Error::Io {
            path: Path::new(".").to_path_buf(),
            source,
        }))
    }

    /// Opens a fresh handle on `dir` and loads it.
    fn reload(dir: &Path) -> PersistentState {
        let mut store = ok(FileStorage::open(dir));
        ok(store.load())
    }

    #[test]
    fn round_trips_metadata_and_log_across_a_reopen() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_metadata(Term::new(4), Some(NodeId::new(3))));
            ok(store.persist_log(LogIndex::new(1), &[entry(1, b"a"), entry(4, b"bb")]));
            ok(store.persist_log(LogIndex::new(3), &[entry(4, b"ccc")]));
        }

        let state = reload(dir.path());
        assert_eq!(state.current_term, Term::new(4));
        assert_eq!(state.voted_for, Some(NodeId::new(3)));
        assert_eq!(
            state.entries,
            vec![entry(1, b"a"), entry(4, b"bb"), entry(4, b"ccc")],
        );
    }

    #[test]
    fn a_fresh_directory_loads_as_empty() {
        let dir = scratch();
        let state = reload(dir.path());
        assert_eq!(state, PersistentState::fresh());
    }

    #[test]
    fn recovery_drops_a_torn_tail_appended_to_the_log() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_log(LogIndex::new(1), &[entry(1, b"a"), entry(1, b"b")]));
        }
        let good_len = ok(
            fs::metadata(dir.path().join("log")).map_err(|source| Error::Io {
                path: dir.path().join("log"),
                source,
            }),
        )
        .len();

        // A half-written third record: a plausible header, no payload.
        let mut log = ok(OpenOptions::new()
            .append(true)
            .open(dir.path().join("log"))
            .map_err(|source| Error::Io {
                path: dir.path().join("log"),
                source,
            }));
        ok(log
            .write_all(&[9, 0, 0, 0, 1, 2, 3, 4])
            .map_err(|source| Error::Io {
                path: dir.path().join("log"),
                source,
            }));
        drop(log);

        let state = reload(dir.path());
        assert_eq!(state.entries, vec![entry(1, b"a"), entry(1, b"b")]);
        // Recovery rewrote the file to just the good prefix.
        let after_len = ok(
            fs::metadata(dir.path().join("log")).map_err(|source| Error::Io {
                path: dir.path().join("log"),
                source,
            }),
        )
        .len();
        assert_eq!(after_len, good_len);
    }

    #[test]
    fn recovery_drops_a_record_with_a_bad_checksum() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_log(LogIndex::new(1), &[entry(1, b"keep"), entry(2, b"rot")]));
        }
        // Flip the last byte of the file: corrupts the second record's payload.
        let path = dir.path().join("log");
        let mut bytes = ok(fs::read(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        }));
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        ok(fs::write(&path, &bytes).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        }));

        assert_eq!(reload(dir.path()).entries, vec![entry(1, b"keep")]);
    }

    #[test]
    fn a_corrupt_metadata_copy_falls_back_to_the_other() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_metadata(Term::new(9), Some(NodeId::new(1))));
        }
        // Truncate meta.0 to nothing; meta.1 still carries the value.
        ok(
            fs::write(dir.path().join("meta.0"), b"").map_err(|source| Error::Io {
                path: dir.path().join("meta.0"),
                source,
            }),
        );

        let state = reload(dir.path());
        assert_eq!(state.current_term, Term::new(9));
        assert_eq!(state.voted_for, Some(NodeId::new(1)));
    }

    #[test]
    fn both_metadata_copies_unreadable_is_a_corrupt_error() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_metadata(Term::new(2), None));
        }
        for name in ["meta.0", "meta.1"] {
            ok(
                fs::write(dir.path().join(name), b"garbage").map_err(|source| Error::Io {
                    path: dir.path().join(name),
                    source,
                }),
            );
        }

        let mut store = ok(FileStorage::open(dir.path()));
        match store.load() {
            Err(Error::Corrupt { .. }) => {}
            other => unreachable!("expected Corrupt, got {other:?}"),
        }
    }

    #[test]
    fn disagreeing_metadata_copies_take_the_higher_term_and_drop_the_vote() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_metadata(Term::new(5), Some(NodeId::new(2))));
        }
        // Rewrite only meta.1 with a newer, disagreeing record (as if a crash
        // landed between the two file writes).
        ok(fs::write(
            dir.path().join("meta.1"),
            encode_metadata(Term::new(7), Some(NodeId::new(4))),
        )
        .map_err(|source| Error::Io {
            path: dir.path().join("meta.1"),
            source,
        }));

        let state = reload(dir.path());
        assert_eq!(state.current_term, Term::new(7));
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn a_config_log_entry_round_trips_across_a_reopen() {
        let dir = scratch();
        let voters = [1, 2, 3, 4].map(NodeId::new);
        let config_entry = LogEntry::config(Term::new(2), ClusterConfig::new(voters));
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_log(
                LogIndex::new(1),
                &[entry(1, b"a"), config_entry.clone(), entry(2, b"c")],
            ));
        }

        let state = reload(dir.path());
        assert_eq!(
            state.entries,
            vec![entry(1, b"a"), config_entry, entry(2, b"c")],
        );
    }

    #[test]
    fn a_snapshot_round_trips_and_trims_the_log_across_a_reopen() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_log(
                LogIndex::new(1),
                &[
                    entry(1, b"a"),
                    entry(1, b"b"),
                    entry(2, b"c"),
                    entry(2, b"d"),
                ],
            ));
            ok(store.persist_snapshot(snap_meta(2, 1), b"state-bytes"));
            // Appends now use global indices past the base.
            ok(store.persist_log(LogIndex::new(5), &[entry(3, b"e")]));
        }

        let state = reload(dir.path());
        let Some(snapshot) = state.snapshot else {
            unreachable!("snapshot recovered");
        };
        assert_eq!(snapshot.meta, snap_meta(2, 1));
        assert_eq!(snapshot.data.as_ref(), b"state-bytes");
        assert_eq!(
            state.entries,
            vec![entry(2, b"c"), entry(2, b"d"), entry(3, b"e")],
        );

        // The on-disk log holds the header plus three records, not seven.
        let log_len = ok(
            fs::metadata(dir.path().join("log")).map_err(|source| Error::Io {
                path: dir.path().join("log"),
                source,
            }),
        )
        .len();
        assert!(log_len < 120, "log not trimmed: {log_len} bytes");
    }

    #[test]
    fn a_snapshot_ahead_of_the_whole_log_clears_it() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_log(LogIndex::new(1), &[entry(1, b"a"), entry(1, b"b")]));
            ok(store.persist_snapshot(snap_meta(9, 4), b"far-ahead"));
            ok(store.persist_log(LogIndex::new(10), &[entry(4, b"j")]));
        }

        let state = reload(dir.path());
        assert_eq!(state.snapshot.map(|s| s.meta), Some(snap_meta(9, 4)));
        assert_eq!(state.entries, vec![entry(4, b"j")]);
    }

    #[test]
    fn load_trims_a_log_head_left_by_a_crash_between_snapshot_and_log_rewrite() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_log(
                LogIndex::new(1),
                &[
                    entry(1, b"a"),
                    entry(1, b"b"),
                    entry(2, b"c"),
                    entry(2, b"d"),
                ],
            ));
        }
        // Simulate the crash: the snapshot file is durable, but the log was
        // never rewritten, so it still holds all four records with header 1.
        ok(fs::write(
            dir.path().join("snapshot"),
            encode_snapshot(&snap_meta(2, 1), b"recovered"),
        )
        .map_err(|source| Error::Io {
            path: dir.path().join("snapshot"),
            source,
        }));

        let state = reload(dir.path());
        assert_eq!(state.snapshot.map(|s| s.meta), Some(snap_meta(2, 1)));
        assert_eq!(state.entries, vec![entry(2, b"c"), entry(2, b"d")]);

        // The trim was written back, so a second reopen is already clean.
        let again = reload(dir.path());
        assert_eq!(again.entries, vec![entry(2, b"c"), entry(2, b"d")]);
    }

    #[test]
    fn a_corrupt_snapshot_is_a_corrupt_error() {
        let dir = scratch();
        {
            let mut store = ok(FileStorage::open(dir.path()));
            ok(store.load());
            ok(store.persist_snapshot(snap_meta(3, 2), b"good"));
        }
        let path = dir.path().join("snapshot");
        let mut bytes = ok(fs::read(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        }));
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        ok(fs::write(&path, &bytes).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        }));

        let mut store = ok(FileStorage::open(dir.path()));
        match store.load() {
            Err(Error::Corrupt { .. }) => {}
            other => unreachable!("expected Corrupt, got {other:?}"),
        }
    }
}
