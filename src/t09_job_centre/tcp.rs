use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
};

use anyhow::Context;
use log::info;
use serde_json::{json, Value};

use crate::{server::TcpServerProblem, t09_job_centre::messages::Request};

use super::{messages::Response, Error};

#[derive(Clone)]
pub struct JobCentre {
    job_server: super::JobServer,
}

impl JobCentre {
    pub fn new() -> JobCentre {
        JobCentre {
            job_server: super::JobServer::new(),
        }
    }

    fn process_line(&self, line: String, client_id: i32) -> Result<Response, Error> {
        info!("Client {client_id} sent line {line:?}");
    
        let val: Value = serde_json::from_str(&line).map_err(|_| Error::InvalidJson)?;
        let request = Request::from_json(&val).ok_or(Error::InvalidFormat)?;
    
        match request {
            Request::Put { queue, job, priority } => {
                let id = self.job_server.put(queue, job, priority);
                Ok(Response::Id { id })
            },
            Request::Get { queues, wait } => {
                if wait {
                    Ok(self.job_server.get_blocking(&queues, client_id))
                } else {
                    Ok(self.job_server.get_nonblocking(&queues, client_id))
                }
            },
            Request::Delete { id } => {
                Ok(self.job_server.delete(id))
            },
            Request::Abort { id } => {
                self.job_server.abort(id, client_id)
            },
        }
    }

    fn handle_disconnect(&self, client_id: i32) {
        info!("Client {client_id} disconnected");
        self.job_server.client_disconnect(client_id);
    }
}

impl TcpServerProblem for JobCentre {
    fn handle_connection(self, mut stream: TcpStream, client_id: i32) -> anyhow::Result<()> {
        info!("Connected to client {client_id}");
        let mut reader = BufReader::new(stream.try_clone().context("cannot clone TcpStream")?);

        loop {
            let mut line = String::new();
            let result = match reader.read_line(&mut line) {
                Ok(0) => {
                    self.handle_disconnect(client_id);
                    break;
                },
                Ok(_) => self.process_line(line, client_id),
                Err(_) => Err(Error::InvalidJson),
            };

            let response = match result {
                Ok(response) => response.to_json(),
                Err(reason) => json!({"status": "error", "reason": reason.to_string()}),
            };

            stream.write_all(format!("{response}\n").as_bytes())?;
        }

        Ok(())
    }
}
