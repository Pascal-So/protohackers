use std::io::{BufRead, BufWriter, Cursor, Write};

use anyhow::anyhow;
use deku::{reader::Reader, DekuContainerWrite, DekuError, DekuRead, DekuReader, DekuWrite};
use thiserror::Error;

use crate::t11_pest_control::domain::types::{self, Population, PopulationRange};

const HELLO_ID: u8 = 0x50;
const ERROR_ID: u8 = 0x51;
const OK_ID: u8 = 0x52;
const DIAL_AUTHORITY_ID: u8 = 0x53;
const TARGET_POPULATIONS_ID: u8 = 0x54;
const CREATE_POLICY_ID: u8 = 0x55;
const DELETE_POLICY_ID: u8 = 0x56;
const POLICY_RESULT_ID: u8 = 0x57;
const SITE_VISIT_ID: u8 = 0x58;

const CULL_ACTION: u8 = 0x90;
const CONSERVE_ACTION: u8 = 0xa0;

const META_BYTES: usize = 6;

#[derive(PartialEq, Eq, Debug)]
pub enum TcpMessage {
    Hello,
    Error {
        message: String,
    },
    Ok,
    DialAuthority {
        site: types::Site,
    },
    TargetPopulations {
        site: types::Site,
        populations: Vec<(types::Species, PopulationRange)>,
    },
    CreatePolicy {
        species: types::Species,
        action: types::PolicyAction,
    },
    DeletePolicy {
        policy_id: types::PolicyId,
    },
    PolicyResult {
        policy_id: types::PolicyId,
    },
    SiteVisit {
        site: types::Site,
        populations: Vec<(types::Species, Population)>,
    },
}

#[derive(Error, Debug)]
pub enum TcpCommunicationError {
    /// Protocol error caused by peer
    #[error("protocol error caused by peer: {0}")]
    PeerProtocolError(String),

    /// Peer says we fucked up
    #[error("peer sent error: {0}")]
    OurProtocolError(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

impl TcpCommunicationError {
    pub fn expected(expected: &str, actual: TcpMessage) -> Self {
        match actual {
            TcpMessage::Error { message } => Self::OurProtocolError(message),
            other => Self::PeerProtocolError(format!("expected {expected} message, got {other:?}")),
        }
    }
}

impl TcpMessage {
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self, TcpCommunicationError> {
        let mut buf = [0u8; 1 << 16];
        let mut bytes_sum = 0u8;

        reader.read_exact(&mut buf[..1])?;
        let message_type = buf[0];
        bytes_sum = bytes_sum.wrapping_add(buf[0]);

        reader.read_exact(&mut buf[..4])?;
        let length = u32::from_be_bytes(buf[..4].try_into().unwrap());
        for b in buf[..4].iter() {
            bytes_sum = bytes_sum.wrapping_add(*b);
        }

        if length < 6 || length > (buf.len() + META_BYTES) as u32 {
            return Err(TcpCommunicationError::PeerProtocolError(format!(
                "invalid message length: {}",
                length
            )));
        }

        let inner_length = length as usize - META_BYTES;
        reader.read_exact(&mut buf[..inner_length])?;
        let content = &mut buf[..inner_length];

        for b in content.iter() {
            bytes_sum = bytes_sum.wrapping_add(*b);
        }

        let mut cursor = Cursor::new(content);
        let mut deku_reader = Reader::new(&mut cursor);
        let msg = DekuTcpMessage::from_reader_with_ctx(&mut deku_reader, message_type).map_err(
            |err| match err {
                DekuError::Io(err) => TcpCommunicationError::IoError(err.into()),
                other => TcpCommunicationError::PeerProtocolError(other.to_string()),
            },
        )?;
        if cursor.position() != inner_length as u64 {
            return Err(TcpCommunicationError::PeerProtocolError(format!("Message specified length ({length}) exceeds the length of the actual message. Only {} bytes of the content were used.", cursor.position())));
        }

        reader.read_exact(&mut buf[..1])?;
        let checksum = buf[0];
        let expected_checksum = 0u8.wrapping_sub(bytes_sum);

        if checksum != expected_checksum {
            return Err(TcpCommunicationError::PeerProtocolError(format!(
                "invalid checksum: {checksum} != {expected_checksum}"
            )));
        }

        let msg = msg.try_into()?;
        log::debug!("received {msg:?}");
        Ok(msg)
    }

    pub fn write<W: Write>(self, writer: &mut BufWriter<W>) -> Result<(), TcpCommunicationError> {
        log::debug!("sending {self:?}");

        let (msg, message_type) = self.into();

        writer.write_all(&[message_type])?;
        let mut sum = message_type;

        let content = msg.to_bytes().map_err(|err| match err {
            DekuError::Io(error) => TcpCommunicationError::IoError(error.into()),
            err => panic!("unexpected deku error: {err}"),
        })?;

        let len = (content.len() + META_BYTES) as u32;
        let len_bytes = len.to_be_bytes();
        writer.write_all(len_bytes.as_slice())?;
        for b in len_bytes.iter() {
            sum = sum.wrapping_add(*b);
        }

        writer.write_all(content.as_slice())?;
        for b in content.iter() {
            sum = sum.wrapping_add(*b);
        }

        let checksum = 0u8.wrapping_sub(sum);
        writer.write_all(&[checksum])?;

        writer.flush()?;

        Ok(())
    }
}

impl From<TcpMessage> for (DekuTcpMessage, u8) {
    fn from(msg: TcpMessage) -> Self {
        match msg {
            TcpMessage::Hello => (
                DekuTcpMessage::Hello {
                    protocol: "pestcontrol".to_string().into(),
                    version: 1,
                },
                HELLO_ID,
            ),
            TcpMessage::Error { message } => (
                DekuTcpMessage::ErrorMsg {
                    message: message.into(),
                },
                ERROR_ID,
            ),
            TcpMessage::Ok => (DekuTcpMessage::Ok, OK_ID),
            TcpMessage::DialAuthority { site } => (
                { DekuTcpMessage::DialAuthority { site_id: site.id } },
                DIAL_AUTHORITY_ID,
            ),
            TcpMessage::TargetPopulations { site, populations } => (
                {
                    DekuTcpMessage::TargetPopulations {
                        site_id: site.id,
                        count: populations.len() as u32,
                        populations: populations
                            .into_iter()
                            .map(|(species, range)| (species.into(), *range.start(), *range.end()))
                            .collect(),
                    }
                },
                TARGET_POPULATIONS_ID,
            ),
            TcpMessage::CreatePolicy { species, action } => (
                DekuTcpMessage::CreatePolicy {
                    species: species.into(),
                    action: match action {
                        types::PolicyAction::Cull => CULL_ACTION,
                        types::PolicyAction::Conserve => CONSERVE_ACTION,
                    },
                },
                CREATE_POLICY_ID,
            ),
            TcpMessage::DeletePolicy { policy_id } => {
                (DekuTcpMessage::DeletePolicy { policy_id }, DELETE_POLICY_ID)
            }
            TcpMessage::PolicyResult { policy_id } => {
                (DekuTcpMessage::PolicyResult { policy_id }, POLICY_RESULT_ID)
            }
            TcpMessage::SiteVisit { site, populations } => (
                DekuTcpMessage::SiteVisit {
                    site_id: site.id,
                    count: populations.len() as u32,
                    populations: populations
                        .into_iter()
                        .map(|(species, count)| (species.into(), count))
                        .collect(),
                },
                SITE_VISIT_ID,
            ),
        }
    }
}

impl TryFrom<DekuTcpMessage> for TcpMessage {
    type Error = TcpCommunicationError;

    fn try_from(msg: DekuTcpMessage) -> Result<Self, TcpCommunicationError> {
        match msg {
            DekuTcpMessage::Hello { protocol, version } => {
                let protocol: String = protocol.try_into()?;
                if protocol != "pestcontrol" {
                    return Err(TcpCommunicationError::PeerProtocolError(format!(
                        "hello message, invalid protocol: {protocol}"
                    )));
                }

                if version != 1 {
                    return Err(TcpCommunicationError::PeerProtocolError(format!(
                        "hello message, invalid version: {version}"
                    )));
                }

                Ok(TcpMessage::Hello)
            }
            DekuTcpMessage::ErrorMsg { message: mesage } => Ok(TcpMessage::Error {
                message: mesage.try_into()?,
            }),
            DekuTcpMessage::Ok => Ok(TcpMessage::Ok),
            DekuTcpMessage::DialAuthority { site_id } => Ok(TcpMessage::DialAuthority {
                site: types::Site { id: site_id },
            }),
            DekuTcpMessage::TargetPopulations {
                site_id,
                populations,
                ..
            } => Ok(TcpMessage::TargetPopulations {
                site: types::Site { id: site_id },
                populations:
                    populations
                        .into_iter()
                        .map(
                            |(species, lower, upper)| -> Result<
                                (types::Species, PopulationRange),
                                TcpCommunicationError,
                            > {
                                Ok((species.try_into()?, lower..=upper))
                            },
                        )
                        .collect::<Result<_, TcpCommunicationError>>()?,
            }),
            DekuTcpMessage::CreatePolicy { species, action } => Ok(TcpMessage::CreatePolicy {
                species: species.try_into()?,
                action: match action {
                    CULL_ACTION => types::PolicyAction::Cull,
                    CONSERVE_ACTION => types::PolicyAction::Conserve,
                    _ => {
                        return Err(TcpCommunicationError::PeerProtocolError(format!(
                            "invalid policy action: {action}"
                        )))
                    }
                },
            }),
            DekuTcpMessage::DeletePolicy { policy_id } => {
                Ok(TcpMessage::DeletePolicy { policy_id })
            }
            DekuTcpMessage::PolicyResult { policy_id } => {
                Ok(TcpMessage::PolicyResult { policy_id })
            }
            DekuTcpMessage::SiteVisit {
                site_id,
                populations,
                ..
            } => Ok(TcpMessage::SiteVisit {
                site: types::Site { id: site_id },
                populations: populations
                    .into_iter()
                    .map(
                        |(species, count)| -> Result<(types::Species, u32), TcpCommunicationError> {
                            Ok((species.try_into()?, count))
                        },
                    )
                    .collect::<Result<_, TcpCommunicationError>>()?,
            }),
        }
    }
}

#[derive(Debug, PartialEq, Eq, DekuRead, DekuWrite)]
#[deku(
    endian = "big",
    ctx = "message_type: u8",
    id = "message_type",
    ctx_default = "99"
)]
// We use ctx_default so that to_bytes() is available on the type, see
// https://github.com/sharksforarms/deku/issues/413#issuecomment-1903994638
//
// This doesn't have any other observable effect because this type does not
// write the message type itself.
enum DekuTcpMessage {
    #[deku(id = "HELLO_ID")]
    Hello { protocol: AsciiString, version: u32 },

    #[deku(id = "ERROR_ID")]
    // we can't name the variant `Error` due to a collision with some deku internals
    ErrorMsg { message: AsciiString },

    #[deku(id = "OK_ID")]
    Ok,

    #[deku(id = "DIAL_AUTHORITY_ID")]
    DialAuthority { site_id: u32 },

    #[deku(id = "TARGET_POPULATIONS_ID")]
    TargetPopulations {
        site_id: u32,
        count: u32,
        #[deku(count = "count")]
        populations: Vec<(AsciiString, u32, u32)>,
    },

    #[deku(id = "CREATE_POLICY_ID")]
    CreatePolicy { species: AsciiString, action: u8 },
    #[deku(id = "DELETE_POLICY_ID")]
    DeletePolicy { policy_id: types::PolicyId },
    #[deku(id = "POLICY_RESULT_ID")]
    PolicyResult { policy_id: types::PolicyId },
    #[deku(id = "SITE_VISIT_ID")]
    SiteVisit {
        site_id: u32,
        count: u32,
        #[deku(count = "count")]
        populations: Vec<(AsciiString, u32)>,
    },
}

#[derive(Debug, PartialEq, Eq, DekuRead, DekuWrite)]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")] // inherit the parent endianness
struct AsciiString {
    char_count: u32,
    #[deku(count = "char_count")]
    content: Vec<u8>,
}

impl TryFrom<AsciiString> for String {
    type Error = TcpCommunicationError;
    fn try_from(ascii_string: AsciiString) -> Result<String, TcpCommunicationError> {
        String::from_utf8(ascii_string.content).or(Err(TcpCommunicationError::PeerProtocolError(
            "invalid utf8".to_string(),
        )))
    }
}

impl From<String> for AsciiString {
    fn from(s: String) -> Self {
        let content = s.into_bytes();
        AsciiString {
            char_count: content.len() as u32,
            content,
        }
    }
}

pub fn handle_protocol_errors<T>(
    result: Result<T, TcpCommunicationError>,
    buf_writer: &mut std::io::BufWriter<std::net::TcpStream>,
) -> Result<T, anyhow::Error> {
    match result {
        Ok(t) => Ok(t),
        Err(TcpCommunicationError::IoError(error)) => Err(anyhow::Error::new(error)),
        Err(TcpCommunicationError::OurProtocolError(message)) => {
            Err(anyhow!("Peer says we fucked up: {message}"))
        }
        Err(TcpCommunicationError::PeerProtocolError(message)) => {
            let error_msg = TcpMessage::Error {
                message: message.clone(),
            };
            if let Err(e) = error_msg.write(buf_writer) {
                log::warn!("could not send error message \"{message}\" to peer: {e}");
            }

            Err(anyhow::Error::msg(message))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;

    use super::*;

    fn read_bytes(bytes: &[u8]) -> Result<TcpMessage, TcpCommunicationError> {
        let mut reader = BufReader::new(bytes);
        TcpMessage::read(&mut reader)
    }

    fn to_bytes(msg: TcpMessage) -> Result<Vec<u8>, TcpCommunicationError> {
        let writer = Vec::new();
        let mut buf_writer = BufWriter::new(writer);
        msg.write(&mut buf_writer)?;
        Ok(buf_writer.into_inner().unwrap())
    }

    #[test]
    fn test_parse_hello() {
        let data = &[
            0x50, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
        ];
        let msg = TcpMessage::Hello;
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_ok() {
        let data = &[0x52, 0x00, 0x00, 0x00, 0x06, 0xa8];
        let msg = TcpMessage::Ok;
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_dial_authority() {
        let data = &[0x53, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x30, 0x39, 0x3a];
        let msg = TcpMessage::DialAuthority {
            site: types::Site { id: 12345 },
        };
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_target_populations() {
        let data = &[
            0x54, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x03, 0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, 0x80,
        ];
        let msg = TcpMessage::TargetPopulations {
            site: types::Site { id: 12345 },
            populations: vec![("dog".to_string(), 1..=3), ("rat".to_string(), 0..=10)],
        };
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_create_policy() {
        let data = &[
            0x55, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0xa0, 0xc0,
        ];
        let msg = TcpMessage::CreatePolicy {
            species: "dog".to_string(),
            action: types::PolicyAction::Conserve,
        };
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_delete_policy() {
        let data = &[0x56, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x7b, 0x25];
        let msg = TcpMessage::DeletePolicy { policy_id: 123 };
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_policy_result() {
        let data = &[0x57, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x7b, 0x24];
        let msg = TcpMessage::PolicyResult { policy_id: 123 };
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }

    #[test]
    fn test_parse_site_visit() {
        let data = &[
            0x58, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
            0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x05, 0x8c,
        ];
        let msg = TcpMessage::SiteVisit {
            site: types::Site { id: 12345 },
            populations: vec![("dog".to_string(), 1), ("rat".to_string(), 5)],
        };
        assert_eq!(read_bytes(data).unwrap(), msg);
        assert_eq!(to_bytes(msg).unwrap(), data);
    }
}
