use std::{collections::HashMap, net::SocketAddr};

use log::debug;
use tokio::{net::UdpSocket, select};

use crate::shutdown::ShutdownToken;

enum Message<'a> {
    Insert(&'a [u8], &'a [u8]),
    Retrieve(&'a [u8]),
}

fn parse_message<'a>(msg: &'a [u8]) -> Message<'a> {
    let pos = msg.iter().position(|c| *c == b'=');
    match pos {
        Some(pos) => Message::Insert(&msg[..pos], &msg[pos + 1..]),
        None => Message::Retrieve(msg),
    }
}

pub async fn server(port: u16, mut shutdown_token: ShutdownToken) -> std::io::Result<()> {
    let mut buf = vec![0u8; 1000];

    let mut store = HashMap::<Vec<u8>, Vec<u8>>::new();

    let socket = UdpSocket::bind(SocketAddr::new([0, 0, 0, 0].into(), port)).await?;

    let version = env!("CARGO_PKG_VERSION").as_bytes().to_owned();
    let empty = Vec::new();

    loop {
        select! {
            res = socket.recv_from(&mut buf) => {
                let (bytes, addr) = res?;
                debug!("-> {:?}", std::str::from_utf8(&buf[..bytes]).unwrap());
                match parse_message(&buf[..bytes]) {
                    Message::Insert(key, val) => {
                        store.insert(key.to_owned(), val.to_owned());
                    },
                    Message::Retrieve(key) => {
                        let mut out = key.to_owned();

                        let val = if key == b"version" {
                            &version
                        } else {
                            store.get(&out).unwrap_or(&empty)
                        };

                        out.push(b'=');
                        out.extend_from_slice(val);
                        debug!("<- {:?}", std::str::from_utf8(&out).unwrap());
                        socket.send_to(&out, addr).await?;
                    }
                }
            }
            _ = shutdown_token.wait_for_shutdown() => return Ok(()),
        }
    }
}
