//! [`FileStorage`]: the real, disk-backed [`Storage`] implementation.
//!
//! Layout inside the data directory:
//!
//! - `log` — an append-only file of records. Each record is
//!   `[u32 LE payload length][u32 LE CRC32C of payload][payload]`, and a
//!   payload is `[u64 LE term][u32 LE command length][command bytes]`.
//!   Recovery reads records until one is short or fails its checksum and
//!   truncates the file there: a torn tail is expected (`AGENTS.md` §8 rule 3).
//! - `meta.0`, `meta.1` — two independent copies of `{ currentTerm, votedFor }`,
//!   each `[u32 LE CRC32C][u64 LE term][u8 has_vote][u64 LE vote id]`. On load a
//!   copy that fails its checksum is ignored; if both parse but disagree the
//!   higher term wins and the vote is dropped (`AGENTS.md` §8 rule 5).
//!
//! Every write `fsync`s the file it touched, and the directory as well when a
//! file was newly created. A failed `fsync` is returned as the fatal
//! [`Error::Sync`].

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::{Error, PersistentState, Storage, crc32c};
use crate::core::{LogEntry, LogIndex, NodeId, Term};

/// The on-disk metadata record is fixed size: crc(4) + term(8) + flag(1) +
/// vote(8).
const META_LEN: usize = 21;

/// Disk-backed durable storage for one Raft server.
///
/// Call [`FileStorage::load`] once before any `persist_*` call: it recovers
/// the log and, in doing so, learns the byte offset of every record, which
/// [`FileStorage::persist_log`] needs in order to truncate.
#[derive(Debug)]
pub struct FileStorage {
    dir: PathBuf,
    log_path: PathBuf,
    log: File,
    /// Byte offset just past record `k`, for every stored record. Its length
    /// is the number of entries currently on disk.
    record_ends: Vec<u64>,
    loaded: bool,
}

impl FileStorage {
    /// Opens (creating if absent) the data directory and the log file.
    ///
    /// On Linux this also logs the data directory's filesystem type and warns
    /// if it is outside the CI-exercised tier (`AGENTS.md` §8).
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
        let log = OpenOptions::new()
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
            sync_dir(&dir)?;
        }

        Ok(Self {
            dir,
            log_path,
            log,
            record_ends: Vec::new(),
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

        let mut entries = Vec::new();
        let mut ends = Vec::new();
        let mut offset: u64 = 0;

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
}

impl Storage for FileStorage {
    fn load(&mut self) -> Result<PersistentState, Error> {
        let (current_term, voted_for) = load_metadata(&self.meta_path(0), &self.meta_path(1))?;
        let entries = self.recover_log()?;
        self.loaded = true;
        Ok(PersistentState {
            current_term,
            voted_for,
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
        // The core only ever passes `1 <= from_index <= len + 1`. Clamp
        // defensively so a bug cannot truncate to a wild offset; the
        // `debug_assert` fails loudly in tests.
        let stored = self.record_ends.len();
        let wanted = from_index.get().saturating_sub(1);
        let keep = usize::try_from(wanted).unwrap_or(usize::MAX).min(stored);
        debug_assert!(
            keep as u64 == wanted,
            "persist_log from_index {} out of range for {stored} stored entries",
            from_index.get(),
        );

        let mut offset = if keep == 0 {
            0
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

/// `[u64 LE term][u32 LE command length][command bytes]`.
fn encode_entry(entry: &LogEntry) -> Result<Vec<u8>, Error> {
    let command_len = u32::try_from(entry.command.len()).map_err(|_| Error::RecordTooLarge {
        bytes: entry.command.len(),
    })?;
    let mut out = Vec::with_capacity(12 + entry.command.len());
    out.extend_from_slice(&entry.term.get().to_le_bytes());
    out.extend_from_slice(&command_len.to_le_bytes());
    out.extend_from_slice(&entry.command);
    Ok(out)
}

fn decode_entry(payload: &[u8]) -> Option<LogEntry> {
    let term = u64::from_le_bytes(payload.get(0..8)?.try_into().ok()?);
    let command_len = u32::from_le_bytes(payload.get(8..12)?.try_into().ok()?) as usize;
    let command = payload.get(12..)?;
    if command.len() != command_len {
        return None;
    }
    Some(LogEntry {
        term: Term::new(term),
        command: Bytes::copy_from_slice(command),
    })
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
        // AGENTS.md §8 rule 5: a copy that fails its checksum is ignored in
        // favour of the readable one.
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

    use super::{FileStorage, encode_metadata};
    use crate::core::{LogEntry, LogIndex, NodeId, Term};
    use crate::storage::{Error, PersistentState, Storage};

    #[track_caller]
    fn ok<T>(result: Result<T, Error>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => unreachable!("expected Ok, got {err}"),
        }
    }

    fn entry(term: u64, cmd: &'static [u8]) -> LogEntry {
        LogEntry {
            term: Term::new(term),
            command: Bytes::from_static(cmd),
        }
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
}
