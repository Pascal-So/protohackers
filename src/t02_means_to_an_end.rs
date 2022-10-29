use std::io::{ErrorKind, Result};

use log::debug;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
};

use crate::shutdown::ShutdownToken;

pub async fn handle_connection(
    mut stream: TcpStream,
    mut shutdown_token: ShutdownToken,
    client_id: i32,
) -> Result<()> {
    let mut buf = vec![0; 9];
    let mut store = vec![];

    loop {
        select! {
            res = stream.read_exact(&mut buf) => if let Err(e) = res {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            },
            _ = shutdown_token.wait_for_shutdown() => break,
        }
        let insert = buf[0] == b'I';
        let v1 = i32::from_be_bytes(buf[1..5].try_into().unwrap());
        let v2 = i32::from_be_bytes(buf[5..9].try_into().unwrap());
        debug!(
            "--> {client_id}: {} {v1} {v2}",
            if insert { 'I' } else { 'Q' }
        );

        if insert {
            store.push((v1, v2));
        } else {
            let mut sum = 0i64;
            let mut count = 0i64;
            for (t, v) in &store {
                if v1 <= *t && *t <= v2 {
                    sum += *v as i64;
                    count += 1;
                }
            }

            let out = if count == 0 { 0 } else { sum / count } as i32;
            debug!("<-- {client_id}: {out}");
            stream.write_all(&out.to_be_bytes()).await?;
        }
    }

    Ok(())
}
