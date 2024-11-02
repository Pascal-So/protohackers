use std::time::Duration;

use anyhow::bail;

use crate::t11_pest_control::{
    domain::types,
    port::authority::{AuthorityServer, AuthoritySession},
};

use super::tcp_message_types::{handle_protocol_errors, TcpCommunicationError, TcpMessage};

pub struct OnlineAuthority {
    url: String,
}

impl OnlineAuthority {
    pub fn new(url: impl Into<String>) -> OnlineAuthority {
        OnlineAuthority { url: url.into() }
    }

    fn dial_inner(
        &self,
        site: types::Site,
        buf_reader: &mut std::io::BufReader<std::net::TcpStream>,
        buf_writer: &mut std::io::BufWriter<std::net::TcpStream>,
    ) -> Result<Vec<types::Target>, TcpCommunicationError> {
        TcpMessage::Hello.write(buf_writer)?;
        match TcpMessage::read(buf_reader)? {
            TcpMessage::Hello => {}
            other => return Err(TcpCommunicationError::expected("Hello", other)),
        }

        TcpMessage::DialAuthority { site }.write(buf_writer)?;
        let targets = match TcpMessage::read(buf_reader)? {
            TcpMessage::TargetPopulations { populations, .. } => populations,
            other => return Err(TcpCommunicationError::expected("TargetPopulations", other)),
        };

        Ok(targets)
    }
}

impl AuthorityServer for OnlineAuthority {
    type Session = OnlineAuthoritySession;

    fn dial(
        &self,
        site: types::Site,
    ) -> Result<(Self::Session, Vec<types::Target>), anyhow::Error> {
        let stream = std::net::TcpStream::connect(&self.url)?;
        stream.set_read_timeout(Some(Duration::from_secs_f32(0.5)))?;
        let mut buf_reader = std::io::BufReader::new(stream.try_clone()?);
        let mut buf_writer = std::io::BufWriter::new(stream);

        let targets_result = self.dial_inner(site, &mut buf_reader, &mut buf_writer);

        let targets = handle_protocol_errors(targets_result, &mut buf_writer)?;

        Ok((
            OnlineAuthoritySession {
                buf_reader,
                buf_writer,
                site,
            },
            targets,
        ))
    }
}

pub struct OnlineAuthoritySession {
    buf_reader: std::io::BufReader<std::net::TcpStream>,
    buf_writer: std::io::BufWriter<std::net::TcpStream>,
    site: types::Site,
}

impl OnlineAuthoritySession {
    fn delete_policy_inner(
        &mut self,
        policy_id: types::PolicyId,
    ) -> Result<(), TcpCommunicationError> {
        TcpMessage::DeletePolicy { policy_id }.write(&mut self.buf_writer)?;
        match TcpMessage::read(&mut self.buf_reader)? {
            TcpMessage::Ok => {}
            other => return Err(TcpCommunicationError::expected("Ok", other)),
        };
        Ok(())
    }
}

impl AuthoritySession for OnlineAuthoritySession {
    fn create_policy(
        &mut self,
        species: types::Species,
        action: types::PolicyAction,
    ) -> Result<types::PolicyId, anyhow::Error> {
        let _span = tracing::info_span!("create_policy", site = self.site.id).entered();

        TcpMessage::CreatePolicy { species, action }.write(&mut self.buf_writer)?;
        let policy_id = match TcpMessage::read(&mut self.buf_reader)? {
            TcpMessage::PolicyResult { policy_id } => policy_id,
            other => {
                bail!("expected PolicyResult from autority, got {other:?}");
            }
        };
        Ok(policy_id)
    }

    fn delete_policy(&mut self, policy_id: types::PolicyId) -> Result<(), anyhow::Error> {
        let _span = tracing::info_span!("delete_policy", site = self.site.id).entered();

        handle_protocol_errors(self.delete_policy_inner(policy_id), &mut self.buf_writer)
    }
}
