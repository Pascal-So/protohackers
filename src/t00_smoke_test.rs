use std::io::Result;

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
    let mut buf = vec![0; 2048];

    loop {
        let bytes = select! {
            bytes = stream.read(&mut buf) => bytes?,
            _ = shutdown_token.wait_for_shutdown() => break,
        };

        if bytes == 0 {
            break;
        }

        debug!("Client {client_id} sent: {:?}", &buf[..bytes]);
        stream.write(&buf[..bytes]).await?;
    }

    Ok(())
}
