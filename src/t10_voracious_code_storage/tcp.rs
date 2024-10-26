use std::io::{BufReader, LineWriter};

use crate::server::TcpServerProblem;

use super::vcs::Vcs;

#[derive(Clone)]
pub struct VcsServer {
    vcs: Vcs,
}

impl VcsServer {
    pub fn new() -> Self {
        Self { vcs: Vcs::new() }
    }
}

impl TcpServerProblem for VcsServer {
    fn handle_connection(self, stream: std::net::TcpStream, client_id: i32) -> anyhow::Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = LineWriter::new(stream);

        self.vcs.session(&mut reader, &mut writer, client_id)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        time::Duration,
    };

    use crate::server::run_tcp_server;

    use super::*;

    struct Client(TcpStream);

    impl Client {
        fn new() -> Self {
            let addr = run_tcp_server(VcsServer::new(), 0);
            let stream = std::net::TcpStream::connect(addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs_f32(0.2)))
                .unwrap();
            Self(stream)
        }

        fn send(&mut self, msg: &[u8]) {
            self.0.write_all(msg).unwrap();
        }

        fn expect(&mut self, msg: &[u8]) {
            let mut buf = vec![0; msg.len()];
            self.0.read_exact(&mut buf).unwrap();
            assert_eq!(
                &buf,
                msg,
                "expected {}, got {}",
                String::from_utf8_lossy(msg),
                String::from_utf8_lossy(buf.as_slice())
            );
        }
    }

    #[test]
    fn test_tcp() {
        let mut client = Client::new();

        client.expect(b"READY\n");
        client.send(b"put foo\n");
        client.expect(b"ERR usage: PUT file length newline data\n");

        client.expect(b"READY\n");
        client.send(b"put foo 4\n");
        client.expect(b"ERR illegal file name\n");

        client.expect(b"READY\n");
        client.send(b"put /foo 4\nasdf");
        client.expect(b"OK r1\n");

        client.expect(b"READY\n");
        client.send(b"get /foo\n");
        client.expect(b"OK 4\nasdf");

        client.expect(b"READY\n");
        client.send(b"help 1\n");
        client.expect(b"OK usage: HELP|GET|PUT|LIST\n");

        client.expect(b"READY\n");
        client.send(b"list /\n");
        client.expect(b"OK 1\n");
        client.expect(b"foo r1\n");

        client.expect(b"READY\n");
        client.send(b"list /foo\n");
        client.expect(b"OK 0\n");
    }
}
