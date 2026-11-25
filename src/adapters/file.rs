//! File-backed stores.
//!
//! * `log.wal`: CRC-framed entry records, appended with `fsync`. Truncation and
//!   compaction rewrite the file atomically (`tmp` + `fsync` + `rename` +
//!   directory `fsync`). A torn or corrupt tail is cut off on load.
//! * `hardstate.bin`, `snapshot.bin`: one CRC-framed record, replaced atomically.

use super::codec::{self, CodecError};
use crate::domain::types::{Entry, HardState, Index, Snapshot};
use crate::ports::{LogStore, PersistedLog, SnapshotStore};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum FileStoreError {
    Io(io::Error),
    Codec(CodecError),
    Corrupt(&'static str),
}

impl fmt::Display for FileStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileStoreError::Io(e) => write!(f, "io: {e}"),
            FileStoreError::Codec(e) => write!(f, "{e}"),
            FileStoreError::Corrupt(w) => write!(f, "corrupt store: {w}"),
        }
    }
}
impl std::error::Error for FileStoreError {}
impl From<io::Error> for FileStoreError {
    fn from(e: io::Error) -> Self {
        FileStoreError::Io(e)
    }
}
impl From<CodecError> for FileStoreError {
    fn from(e: CodecError) -> Self {
        FileStoreError::Codec(e)
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    // Directory fsync makes the rename durable on POSIX; best effort elsewhere.
    match File::open(dir) {
        Ok(d) => d.sync_all().or(Ok(())),
        Err(_) => Ok(()),
    }
}

fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

fn read_single_record(path: &Path) -> Result<Option<Vec<u8>>, FileStoreError> {
    let mut buf = Vec::new();
    match File::open(path) {
        Ok(mut f) => f.read_to_end(&mut buf)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let (recs, end) = codec::unframe_all(&buf);
    if recs.len() != 1 || end != buf.len() {
        return Err(FileStoreError::Corrupt("single-record file damaged"));
    }
    Ok(Some(recs[0].to_vec()))
}

const LOG: &str = "log.wal";
const HS: &str = "hardstate.bin";
const SNAP: &str = "snapshot.bin";

pub struct FileLogStore {
    dir: PathBuf,
    file: File,
    /// Mirror of what is on disk, used to rewrite on truncate/compact.
    entries: Vec<Entry>,
    loaded: bool,
}

impl FileLogStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, FileStoreError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(dir.join(LOG))?;
        Ok(Self {
            dir,
            file,
            entries: Vec::new(),
            loaded: false,
        })
    }

    fn rewrite(&mut self) -> Result<(), FileStoreError> {
        let mut bytes = Vec::new();
        for e in &self.entries {
            bytes.extend(codec::frame(&codec::encode_entry(e)));
        }
        write_atomic(&self.dir, LOG, &bytes)?;
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(self.dir.join(LOG))?;
        Ok(())
    }

    fn ensure_loaded(&mut self) -> Result<(), FileStoreError> {
        if !self.loaded {
            self.load()?;
        }
        Ok(())
    }
}

impl LogStore for FileLogStore {
    type Error = FileStoreError;

    fn load(&mut self) -> Result<PersistedLog, FileStoreError> {
        let mut buf = Vec::new();
        File::open(self.dir.join(LOG))?.read_to_end(&mut buf)?;
        let (recs, end) = codec::unframe_all(&buf);
        let mut entries: Vec<Entry> = Vec::with_capacity(recs.len());
        for r in recs {
            let e = codec::decode_entry(r)?;
            if let Some(prev) = entries.last() {
                if e.index != prev.index + 1 {
                    return Err(FileStoreError::Corrupt("non-contiguous log"));
                }
            }
            entries.push(e);
        }
        self.entries = entries;
        self.loaded = true;
        if end != buf.len() {
            // Torn write from a crash: drop the partial tail durably.
            self.rewrite()?;
        }
        let hard_state = match read_single_record(&self.dir.join(HS))? {
            Some(b) => codec::decode_hard_state(&b)?,
            None => HardState::default(),
        };
        Ok(PersistedLog {
            hard_state,
            entries: self.entries.clone(),
        })
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), FileStoreError> {
        self.ensure_loaded()?;
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let needs_truncate = self.entries.last().is_some_and(|l| first.index <= l.index);
        if needs_truncate {
            self.entries.retain(|e| e.index < first.index);
            self.entries.extend_from_slice(entries);
            return self.rewrite();
        }
        let mut bytes = Vec::new();
        for e in entries {
            bytes.extend(codec::frame(&codec::encode_entry(e)));
        }
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn truncate_from(&mut self, index: Index) -> Result<(), FileStoreError> {
        self.ensure_loaded()?;
        let before = self.entries.len();
        self.entries.retain(|e| e.index < index);
        if self.entries.len() != before {
            self.rewrite()?;
        }
        Ok(())
    }

    fn compact(&mut self, index: Index) -> Result<(), FileStoreError> {
        self.ensure_loaded()?;
        let before = self.entries.len();
        self.entries.retain(|e| e.index > index);
        if self.entries.len() != before {
            self.rewrite()?;
        }
        Ok(())
    }

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), FileStoreError> {
        write_atomic(&self.dir, HS, &codec::frame(&codec::encode_hard_state(hs)))?;
        Ok(())
    }
}

pub struct FileSnapshotStore {
    dir: PathBuf,
}

impl FileSnapshotStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, FileStoreError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }
}

impl SnapshotStore for FileSnapshotStore {
    type Error = FileStoreError;

    fn save(&mut self, s: &Snapshot) -> Result<(), FileStoreError> {
        write_atomic(&self.dir, SNAP, &codec::frame(&codec::encode_snapshot(s)))?;
        Ok(())
    }

    fn load(&mut self) -> Result<Option<Snapshot>, FileStoreError> {
        match read_single_record(&self.dir.join(SNAP))? {
            Some(b) => Ok(Some(codec::decode_snapshot(&b)?)),
            None => Ok(None),
        }
    }
}
