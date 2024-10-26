use std::io::{BufRead, Write};

use thiserror::Error;

use super::storage::{Data, DirName, FileName, ListingEntry, Revision, Storage, StorageError};

#[derive(Clone)]
pub struct Vcs {
    storage: Storage,
}

impl Vcs {
    pub fn new() -> Self {
        Self {
            storage: Storage::new(),
        }
    }

    pub fn session(
        &self,
        reader: &mut impl BufRead,
        writer: &mut impl Write,
        client_id: i32,
    ) -> std::io::Result<()> {
        loop {
            writer.write_all(b"READY\n")?;
            let method = Method::read(reader);
            log::info!("  got method from client {client_id}: {method:?}");

            let result = match method {
                Ok(Method::Help) => Ok(Response::Help),
                Ok(Method::Get {
                    file_name,
                    revision,
                }) => self
                    .storage
                    .get(&file_name, revision)
                    .map(Response::FileContent)
                    .map_err(Error::StorageError),
                Ok(Method::Put { file_name, data }) => {
                    let revision = self.storage.put(file_name, data);
                    revision
                        .map(Response::Revision)
                        .map_err(Error::StorageError)
                }
                Ok(Method::List { dir_name }) => {
                    let entries = self.storage.list(&dir_name);
                    Ok(Response::Listing(entries))
                }
                Err(Error::IoError(e)) => {
                    return Err(e);
                }
                Err(err) => Err(err.into()),
            };

            log::info!("  sending response to client {client_id}: {result:?}");

            match result {
                Ok(response) => {
                    response.write(writer)?;
                }
                Err(err @ Error::IllegalMethod(_)) => {
                    err.write(writer)?;
                    return Ok(());
                }
                Err(err) => {
                    err.write(writer)?;
                }
            }
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
enum Method {
    Help,
    Get {
        file_name: FileName,
        revision: Option<Revision>,
    },
    Put {
        file_name: FileName,
        data: Data,
    },
    List {
        dir_name: DirName,
    },
}

#[derive(Debug)]
enum Response {
    Help,
    Listing(Vec<ListingEntry>),
    FileContent(Data),
    Revision(Revision),
}

#[derive(Debug, Error)]
enum Error {
    #[error("usage get")]
    UsageGet,
    #[error("usage put")]
    UsagePut,
    #[error("usage list")]
    UsageList,
    #[error("illegal method")]
    IllegalMethod(Vec<u8>),
    #[error("illegal file name")]
    IllegalFileName,
    #[error("illegal dir name")]
    IllegalDirName,
    #[error("storage error")]
    StorageError(StorageError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

impl Method {
    fn read(reader: &mut impl BufRead) -> Result<Self, Error> {
        let mut line = Vec::with_capacity(64);
        reader.read_until(b'\n', &mut line)?;
        if line.last() == Some(&b'\n') {
            line.pop();
        }

        let parts = line.split(|b| *b == b' ').collect::<Vec<_>>();

        let method: Vec<u8> = parts[0].iter().map(|c| c.to_ascii_lowercase()).collect();

        match &*method {
            b"get" => {
                if parts.len() > 3 {
                    return Err(Error::UsageGet);
                }

                let name = parts.get(1).ok_or(Error::UsageGet)?;
                let file_name = FileName::new(name).ok_or(Error::IllegalFileName)?;
                let revision = parts.get(2).map(|part| Revision::new(part));

                Ok(Method::Get {
                    file_name,
                    revision,
                })
            }
            b"put" => {
                if parts.len() != 3 {
                    return Err(Error::UsagePut);
                }

                let name = parts.get(1).ok_or(Error::UsagePut)?;
                let file_name = FileName::new(name).ok_or(Error::IllegalFileName)?;

                let length = parts.get(2).ok_or(Error::UsagePut)?;
                let length: usize = match std::str::from_utf8(length) {
                    Ok(s) => s.parse().unwrap_or(0),
                    Err(_) => 0,
                };

                let mut data = vec![0; length];
                reader.read_exact(&mut data)?;

                Ok(Method::Put { file_name, data })
            }
            b"list" => {
                if parts.len() != 2 {
                    return Err(Error::UsageList);
                }

                let name = parts.get(1).ok_or(Error::UsageList)?;
                let dir_name = DirName::new(name).ok_or(Error::IllegalDirName)?;

                Ok(Method::List { dir_name })
            }
            b"help" => Ok(Method::Help),
            _ => Err(Error::IllegalMethod(method)),
        }
    }
}

impl Error {
    fn write(&self, writer: &mut impl Write) -> std::io::Result<()> {
        writer.write_all(b"ERR ")?;
        match &self {
            Error::UsageGet => writer.write_all(b"usage: GET file [revision]\n"),
            Error::UsagePut => writer.write_all(b"usage: PUT file length newline data\n"),
            Error::UsageList => writer.write_all(b"usage: LIST dir\n"),
            Error::IllegalMethod(name) => {
                writer.write_all(b"illegal method: ")?;
                writer.write_all(name)?;
                writer.write_all(b"\n")
            }
            Error::IllegalFileName => writer.write_all(b"illegal file name\n"),
            Error::IllegalDirName => writer.write_all(b"illegal dir name\n"),
            Error::StorageError(StorageError::NoSuchFile) => writer.write_all(b"no such file\n"),
            Error::StorageError(StorageError::NoSuchRevision) => {
                writer.write_all(b"no such revision\n")
            }
            Error::StorageError(StorageError::NotATextFile) => {
                writer.write_all(b"text files only\n")
            }
            Error::IoError(_) => writer.write_all(b"internal error\n"),
        }
    }
}

impl Response {
    fn write(&self, writer: &mut impl Write) -> std::io::Result<()> {
        writer.write_all(b"OK ")?;
        match &self {
            Response::Help => writer.write_all(b"usage: HELP|GET|PUT|LIST\n"),
            Response::Listing(entries) => {
                write!(writer, "{}\n", entries.len())?;

                for entry in entries {
                    match entry {
                        ListingEntry::File { name, revision } => {
                            writer.write_all(name)?;
                            write!(writer, " {revision}\n")?;
                        }
                        ListingEntry::Dir { name } => {
                            writer.write_all(name)?;
                            write!(writer, "/ DIR\n")?;
                        }
                    }
                }
                Ok(())
            }
            Response::FileContent(content) => {
                write!(writer, "{}\n", content.len())?;
                writer.write_all(content)
            }
            Response::Revision(revision) => write!(writer, "{revision}\n"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method_reader() {
        let method = Method::read(&mut b"help plz\n".as_slice()).unwrap();
        assert_eq!(method, Method::Help);

        let method = Method::read(&mut b"get /foo r2\n".as_slice()).unwrap();
        assert_eq!(
            method,
            Method::Get {
                file_name: FileName::new(b"/foo").unwrap(),
                revision: Some(Revision::new(b"r2")),
            }
        );

        let method = Method::read(&mut b"get /foo\n".as_slice()).unwrap();
        assert_eq!(
            method,
            Method::Get {
                file_name: FileName::new(b"/foo").unwrap(),
                revision: None,
            }
        );

        let method = Method::read(&mut b"PUT /foo/bar 4\nasdf".as_slice()).unwrap();
        assert_eq!(
            method,
            Method::Put {
                file_name: FileName::new(b"/foo/bar").unwrap(),
                data: b"asdf".to_vec(),
            }
        );

        let method = Method::read(&mut b"LIST /foo\n".as_slice()).unwrap();
        assert_eq!(
            method,
            Method::List {
                dir_name: DirName::new(b"/foo").unwrap()
            }
        );

        let method = Method::read(&mut b"foo\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::IllegalMethod(_)));

        let method = Method::read(&mut b"\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::IllegalMethod(_)));

        let method = Method::read(&mut b"get\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::UsageGet));

        let method = Method::read(&mut b"put a b c d e\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::UsagePut));

        let method = Method::read(&mut b"list /a b\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::UsageList));

        let method = Method::read(&mut b"gEt a\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::IllegalFileName));

        let method = Method::read(&mut b"list a/b\n".as_slice()).unwrap_err();
        assert!(matches!(method, Error::IllegalDirName));
    }
}
