use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    server::TcpServerProblem,
    t11_pest_control::{
        domain::{Controller, SiteVisitError},
        port::authority::AuthorityServer,
    },
};

use super::adapter::tcp_message_types::{
    handle_protocol_errors, TcpCommunicationError, TcpMessage,
};

pub struct PestControlServer<Authority: AuthorityServer> {
    controller: Arc<Mutex<Controller<Authority>>>,
}

impl<Authority: AuthorityServer> PestControlServer<Authority> {
    pub fn new(authority: Authority) -> Self {
        Self {
            controller: Arc::new(Mutex::new(Controller::new(authority))),
        }
    }

    fn handle_connection_inner(
        &self,
        buf_reader: &mut std::io::BufReader<std::net::TcpStream>,
        buf_writer: &mut std::io::BufWriter<std::net::TcpStream>,
    ) -> Result<(), TcpCommunicationError> {
        match TcpMessage::read(buf_reader)? {
            TcpMessage::Hello => {}
            other => {
                return Err(TcpCommunicationError::ProtocolError(format!(
                    "expected hello from client, got {other:?}"
                )))
            }
        }
        TcpMessage::Hello.write(buf_writer)?;

        let (site, populations) = match TcpMessage::read(buf_reader)? {
            TcpMessage::SiteVisit { site, populations } => (site, populations),
            other => {
                return Err(TcpCommunicationError::ProtocolError(format!(
                    "expected hello from client, got {other:?}"
                )))
            }
        };

        let result = self
            .controller
            .lock()
            .unwrap()
            .site_visit(site, populations);

        match result {
            Ok(()) => {}
            Err(SiteVisitError::DuplicateObservations) => {
                return Err(TcpCommunicationError::ProtocolError(
                    "duplicate observations".to_string(),
                ))
            }
            Err(SiteVisitError::InternalError(err)) => {
                log::error!("Error during site visit: {err}");
                // the client doesn't expect a response here, even in the case of an error.
            }
        }

        todo!()
    }
}

// we can't use derive(Clone) because the autogenerated impl is too restrictive. See
// https://ngr.yt/blog/rust-copy-clone-too-restrictive/ and https://github.com/rust-lang/rust/issues/26925
impl<Authority: AuthorityServer> Clone for PestControlServer<Authority> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
        }
    }
}

impl<Authority: AuthorityServer> TcpServerProblem for PestControlServer<Authority>
where
    PestControlServer<Authority>: Clone,
    Authority: Send + 'static,
    Authority::Session: Send,
{
    fn handle_connection(self, stream: std::net::TcpStream, _client_id: i32) -> anyhow::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs_f32(0.5)))?;
        let mut buf_reader = std::io::BufReader::new(stream.try_clone()?);
        let mut buf_writer = std::io::BufWriter::new(stream);

        let result = self.handle_connection_inner(&mut buf_reader, &mut buf_writer);

        handle_protocol_errors(result, &mut buf_writer)
    }
}
