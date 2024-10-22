use std::{
    io::{Read, Write},
    net::TcpStream,
};

use log::debug;

use crate::server::TcpServerProblem;

struct EchoServer;

impl TcpServerProblem for EchoServer {
    type SharedState = ();

    fn handle_connection(
        mut stream: TcpStream,
        _client_id: i32,
        _state: Self::SharedState,
    ) -> anyhow::Result<()> {
        let mut buf = [0; 1024];
        loop {
            let bytes = stream.read(&mut buf)?;
            debug!("EchoServer: read {bytes} bytes");
            stream.write_all(&buf[..bytes])?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;

    use crate::server::run_tcp_server;

    use super::*;

    #[test]
    fn smoke_test() {
        let port = 9910;
        run_tcp_server::<EchoServer>(port, ());

        let mut client = TcpStream::connect(("localhost", port)).unwrap();

        let mut buf = [0; 100];
        client.write("hello".as_bytes()).unwrap();
        assert_eq!(client.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[0..5], "hello".as_bytes());

        client.write("world".as_bytes()).unwrap();
        assert_eq!(client.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[0..5], "world".as_bytes());
    }

    #[test]
    fn simultaneous_clients() {
        let port = 9920;
        run_tcp_server::<EchoServer>(port, ());

        let mut client_a = TcpStream::connect(("localhost", port)).unwrap();
        let mut client_b = TcpStream::connect(("localhost", port)).unwrap();

        let mut buf = [0; 100];
        client_a.write("hello".as_bytes()).unwrap();
        client_b.write("world".as_bytes()).unwrap();
        assert_eq!(client_a.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[..5], "hello".as_bytes());
        assert_eq!(client_b.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf[..5], "world".as_bytes());
    }
}
