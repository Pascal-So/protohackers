use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Storage {
    files: Arc<Mutex<BTreeMap<FileName, Vec<Data>>>>,
}

pub type Data = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirName {
    name_segments: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileName {
    dir_name: DirName,
    name: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Revision(Option<i32>);

#[derive(Debug, PartialEq, Eq, Ord)]
pub enum ListingEntry {
    File { name: Vec<u8>, revision: Revision },
    Dir { name: Vec<u8> },
}

#[derive(Debug, PartialEq, Eq)]
pub enum StorageError {
    NoSuchFile,
    NoSuchRevision,
    NotATextFile,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn get(
        &self,
        file_name: &FileName,
        revision: Option<Revision>,
    ) -> Result<Vec<u8>, StorageError> {
        let map = self.files.lock().unwrap();

        let revisions = map.get(file_name).ok_or(StorageError::NoSuchFile)?;

        let data = if let Some(revision) = revision {
            let Some(revision_index) = revision.index() else {
                return Err(StorageError::NoSuchRevision);
            };

            revisions
                .get(revision_index)
                .ok_or(StorageError::NoSuchFile)
        } else {
            revisions.last().ok_or(StorageError::NoSuchFile)
        }?;

        Ok(data.clone())
    }

    pub fn put(&self, file_name: FileName, data: Vec<u8>) -> Result<Revision, StorageError> {
        if std::str::from_utf8(&*data).is_err() {
            return Err(StorageError::NotATextFile);
        }

        let mut map = self.files.lock().unwrap();

        let revisions = map.entry(file_name).or_default();
        if revisions.last() == Some(&data) {
            return Ok(Revision::from_index(revisions.len() - 1));
        }
        revisions.push(data);

        Ok(Revision::from_index(revisions.len() - 1))
    }

    pub fn list(&self, dir_name: &DirName) -> Vec<ListingEntry> {
        let map = self.files.lock().unwrap();

        let mut entries = map
            .iter()
            .filter_map(|(file_name, revisions)| {
                if file_name.is_direct_child(dir_name) {
                    Some(ListingEntry::from_file(
                        file_name,
                        Revision::from_index(revisions.len() - 1),
                    ))
                } else if let Some(name) = file_name.get_immediate_child_dir_of(&dir_name) {
                    Some(ListingEntry::Dir { name })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        entries.sort();
        entries.dedup_by_key(|entry| entry.name().to_vec());

        entries
    }
}

const VALID_NAME_CHARS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.";

impl DirName {
    pub fn new(name: &[u8]) -> Option<Self> {
        if name.len() > 1024 {
            return None;
        }

        let mut split = name.split(|c| *c == b'/');

        let first = split.next()?;
        if !first.is_empty() {
            return None;
        }

        let mut segments: Vec<&[u8]> = split.collect();
        if segments.last()? == &b"" {
            segments.pop();
        }

        let segments: Option<Vec<Vec<u8>>> = segments
            .into_iter()
            .map(|segment| {
                if !segment.is_empty() && segment.iter().all(|c| VALID_NAME_CHARS.contains(c)) {
                    Some(segment.to_vec())
                } else {
                    None
                }
            })
            .collect();

        Some(Self {
            name_segments: segments?,
        })
    }
}

impl FileName {
    pub fn new(name: &[u8]) -> Option<Self> {
        let mut rsplit = name.rsplitn(2, |c| *c == b'/');

        let file = rsplit.next()?;
        let mut dir = rsplit.next()?;

        if dir == b"/" {
            return None;
        }
        if dir.is_empty() {
            dir = b"/";
        }
        let dir = DirName::new(dir)?;

        let valid = file.iter().all(|c| VALID_NAME_CHARS.contains(c))
            && file.len() > 0
            && file.len() < 1024;

        if valid {
            Some(Self {
                dir_name: dir,
                name: file.to_vec(),
            })
        } else {
            None
        }
    }

    pub fn is_direct_child(&self, dir: &DirName) -> bool {
        self.dir_name == *dir
    }

    pub fn get_immediate_child_dir_of(
        &self,
        DirName {
            name_segments: parent_segments,
        }: &DirName,
    ) -> Option<Vec<u8>> {
        let child_segments = &self.dir_name.name_segments;
        if child_segments.len() < parent_segments.len()
            || child_segments[..parent_segments.len()] != parent_segments[..]
        {
            return None;
        }

        child_segments
            .get(parent_segments.len())
            .map(|segment| segment.to_vec())
    }
}

impl Revision {
    pub fn new(revision: &[u8]) -> Self {
        fn inner(mut revision: &[u8]) -> Option<i32> {
            if revision.first() == Some(&b'r') {
                revision = &revision[1..];
            }

            let revision = revision
                .into_iter()
                .take_while(|c| c.is_ascii_digit())
                .copied()
                .collect();

            String::from_utf8(revision).ok()?.parse().ok()
        }

        let revision_number = inner(revision).unwrap_or(0);

        if revision_number > 0 {
            Self(Some(revision_number))
        } else {
            Self(None)
        }
    }

    pub fn from_index(index: usize) -> Self {
        Self(Some(index as i32 + 1))
    }

    pub fn index(&self) -> Option<usize> {
        self.0.map(|r| r as usize - 1)
    }
}

impl std::fmt::Display for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(revision) => write!(f, "r{}", revision),
            None => write!(f, "unknown_revision"),
        }
    }
}

impl ListingEntry {
    pub fn from_file(file: &FileName, revision: Revision) -> Self {
        Self::File {
            name: file.name.clone(),
            revision,
        }
    }

    fn name(&self) -> &[u8] {
        match self {
            ListingEntry::File { name, .. } => name,
            ListingEntry::Dir { name } => name,
        }
    }
}

impl PartialOrd for ListingEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let cmp = self.name().partial_cmp(other.name());

        if cmp == Some(std::cmp::Ordering::Equal) {
            if matches!(
                (self, other),
                (ListingEntry::File { .. }, ListingEntry::Dir { .. })
            ) {
                Some(std::cmp::Ordering::Less)
            } else if matches!(
                (self, other),
                (ListingEntry::Dir { .. }, ListingEntry::File { .. })
            ) {
                Some(std::cmp::Ordering::Greater)
            } else {
                None
            }
        } else {
            cmp
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage() {
        let storage = Storage::new();

        let foo = FileName::new(b"/foo").unwrap();
        let root = DirName::new(b"/").unwrap();
        let data = b"asdf".to_vec();

        assert_eq!(storage.get(&foo, None), Err(StorageError::NoSuchFile));
        assert_eq!(
            storage.put(foo.clone(), data.clone()),
            Ok(Revision::new(b"r1"))
        );

        assert_eq!(storage.get(&foo, None), Ok(data.clone()));
        assert_eq!(
            storage.get(&foo, Some(Revision::new(b"r1"))),
            Ok(data.clone())
        );
        assert_eq!(
            storage.get(&foo, Some(Revision::new(b"r0"))),
            Err(StorageError::NoSuchRevision)
        );

        assert_eq!(
            storage.list(&root),
            vec![ListingEntry::File {
                name: b"foo".to_vec(),
                revision: Revision::new(b"r1")
            }]
        );
    }

    #[test]
    fn test_storage_revisions() {
        let storage = Storage::new();

        assert_eq!(
            storage.put(FileName::new(b"/foo").unwrap(), b"asdf".to_vec()),
            Ok(Revision::new(b"r1"))
        );
        assert_eq!(
            storage.put(FileName::new(b"/foo").unwrap(), b"asdf".to_vec()),
            Ok(Revision::new(b"r1"))
        );
        assert_eq!(
            storage.put(FileName::new(b"/foo").unwrap(), b"something else".to_vec()),
            Ok(Revision::new(b"r2"))
        );
        assert_eq!(
            storage.put(FileName::new(b"/foo").unwrap(), b"asdf".to_vec()),
            Ok(Revision::new(b"r3"))
        );
    }

    #[test]
    fn test_reject_non_utf8() {
        let storage = Storage::new();

        let non_utf8 = vec![0xC2, 0x00];
        assert!(std::str::from_utf8(&non_utf8).is_err());

        let file = FileName::new(b"/foo").unwrap();

        assert_eq!(
            storage.put(file.clone(), non_utf8.clone()),
            Err(StorageError::NotATextFile),
        );
    }

    #[test]
    fn test_dir_listing() {
        let storage = Storage::new();
        storage
            .put(FileName::new(b"/dir_a/file").unwrap(), b"asdf".to_vec())
            .unwrap();
        storage
            .put(
                FileName::new(b"/dir_a/dir_b1/dir_c1/file_d").unwrap(),
                b"asdf".to_vec(),
            )
            .unwrap();
        storage
            .put(
                FileName::new(b"/dir_a/dir_b3/file_c3").unwrap(),
                b"asdf".to_vec(),
            )
            .unwrap();
        storage
            .put(
                FileName::new(b"/dir_a/dir_b2/file_c2").unwrap(),
                b"asdf".to_vec(),
            )
            .unwrap();

        assert_eq!(
            storage.list(&DirName::new(b"/").unwrap()),
            vec![ListingEntry::Dir {
                name: b"dir_a".to_vec()
            }]
        );

        assert_eq!(
            storage.list(&DirName::new(b"/dir_a").unwrap()),
            vec![
                ListingEntry::Dir {
                    name: b"dir_b1".to_vec()
                },
                ListingEntry::Dir {
                    name: b"dir_b2".to_vec()
                },
                ListingEntry::Dir {
                    name: b"dir_b3".to_vec()
                },
                ListingEntry::File {
                    name: b"file".to_vec(),
                    revision: Revision::new(b"r1")
                }
            ]
        );

        assert_eq!(
            storage.list(&DirName::new(b"/dir_a/dir_b1/dir_c1").unwrap()),
            vec![ListingEntry::File {
                name: b"file_d".to_vec(),
                revision: Revision::new(b"r1")
            }]
        );
    }

    #[test]
    fn test_file_shadows_dir_in_listing() {
        let storage = Storage::new();

        storage
            .put(
                FileName::new(b"/dir_or_file/file").unwrap(),
                b"asdf".to_vec(),
            )
            .unwrap();
        storage
            .put(FileName::new(b"/dir_or_file").unwrap(), b"asdf".to_vec())
            .unwrap();

        assert_eq!(
            storage.list(&DirName::new(b"/").unwrap()),
            vec![ListingEntry::File {
                name: b"dir_or_file".to_vec(),
                revision: Revision::new(b"r1")
            }]
        );
    }

    #[test]
    fn test_dirname() {
        assert!(DirName::new(b"").is_none());
        assert!(DirName::new(b"//").is_none());
        assert!(DirName::new(b"/a//").is_none());
        assert!(DirName::new(b"//a/").is_none());
        assert!(DirName::new(b"//a").is_none());
        assert!(DirName::new(b"/a?").is_none());

        assert_eq!(
            DirName::new(b"/"),
            Some(DirName {
                name_segments: vec![],
            })
        );
        assert_eq!(
            DirName::new(b"/a/"),
            Some(DirName {
                name_segments: vec![b"a".to_vec()],
            })
        );
        assert_eq!(
            DirName::new(b"/a"),
            Some(DirName {
                name_segments: vec![b"a".to_vec()],
            })
        );
        assert_eq!(
            DirName::new(b"/a/b/"),
            Some(DirName {
                name_segments: vec![b"a".to_vec(), b"b".to_vec()],
            })
        );
        assert_eq!(
            DirName::new(b"/a/b"),
            Some(DirName {
                name_segments: vec![b"a".to_vec(), b"b".to_vec()],
            })
        );
    }

    #[test]
    fn test_filename() {
        assert!(FileName::new(b"").is_none());
        assert!(FileName::new(b"/").is_none());
        assert!(FileName::new(b"//").is_none());
        assert!(FileName::new(b"/a/").is_none());
        assert!(FileName::new(b"/a//").is_none());
        assert!(FileName::new(b"//a/").is_none());
        assert!(FileName::new(b"//a").is_none());
        assert!(FileName::new(b"/a?").is_none());
        assert!(FileName::new(b"/\xD1\x8D").is_none());

        assert_eq!(
            FileName::new(b"/a"),
            Some(FileName {
                dir_name: DirName::new(b"/").unwrap(),
                name: b"a".to_vec()
            })
        );
        assert_eq!(
            FileName::new(b"/a/b/c/d/e/f/g"),
            Some(FileName {
                dir_name: DirName::new(b"/a/b/c/d/e/f").unwrap(),
                name: b"g".to_vec()
            })
        );
    }

    #[test]
    fn test_revision() {
        assert_eq!(Revision::new(b"").0, None);
        assert_eq!(Revision::new(b"r0").0, None);
        assert_eq!(Revision::new(b"r1").0, Some(1));
        assert_eq!(Revision::new(b"r20").0, Some(20));
        assert_eq!(Revision::new(b"r020").0, Some(20));
        assert_eq!(Revision::new(b"r000020").0, Some(20));
        assert_eq!(Revision::new(b"r1a").0, Some(1));
        assert_eq!(Revision::new(b"r1#^$").0, Some(1));
        assert_eq!(Revision::new(b"R1").0, None);
        assert_eq!(Revision::new(b"rr1").0, None);
        assert_eq!(Revision::new(b"\xD1\x8D").0, None);

        assert_eq!(Revision::new(b"r1").index(), Some(0));

        assert_eq!(format!("{}", Revision::from_index(5)), "r6");
    }

    #[test]
    fn test_child() {
        assert_eq!(
            FileName::new(b"/a")
                .unwrap()
                .is_direct_child(&DirName::new(b"/").unwrap()),
            true
        );
        assert_eq!(
            FileName::new(b"/a")
                .unwrap()
                .is_direct_child(&DirName::new(b"/a").unwrap()),
            false
        );
        assert_eq!(
            FileName::new(b"/a")
                .unwrap()
                .is_direct_child(&DirName::new(b"/a/b/").unwrap()),
            false
        );
        assert_eq!(
            FileName::new(b"/a/b")
                .unwrap()
                .is_direct_child(&DirName::new(b"/").unwrap()),
            false
        );
        assert_eq!(
            FileName::new(b"/a/b/c/d")
                .unwrap()
                .is_direct_child(&DirName::new(b"/").unwrap()),
            false
        );
        assert_eq!(
            FileName::new(b"/a/b")
                .unwrap()
                .is_direct_child(&DirName::new(b"/a").unwrap()),
            true
        );
        assert_eq!(
            FileName::new(b"/a/b/c")
                .unwrap()
                .is_direct_child(&DirName::new(b"/a").unwrap()),
            false
        );
        assert_eq!(
            FileName::new(b"/a/b/c")
                .unwrap()
                .is_direct_child(&DirName::new(b"/a/b").unwrap()),
            true
        );
    }
}
