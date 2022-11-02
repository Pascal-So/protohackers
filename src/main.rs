mod server;
mod shutdown;
mod t00_smoke_test;
mod t01_prime_time;
mod t02_means_to_an_end;
mod t03_budget_chat;
mod t04_unusual_database_program;
mod t05_mob_in_the_middle;
mod t06_speed_daemon;

use std::net::SocketAddr;

use log::{info, warn};
use tokio::{net::TcpListener, select, signal, spawn};

use crate::{shutdown::ShutdownController, t03_budget_chat::State};

#[tokio::main]
async fn main() {
    env_logger::init();

    let port: u16 = match std::env::args().nth(1) {
        Some(str) => match str.parse() {
            Ok(p) => p,
            Err(_) => panic!("Invalid port number: {str}"),
        },
        None => 8080,
    };
    info!("Starting server on port {port}");


    let shutdown = ShutdownController::new();

    // t04_unusual_database_program::server(port, shutdown.token())
    //     .await
    //     .unwrap();
    // return;

    let listener = TcpListener::bind(SocketAddr::new([0, 0, 0, 0].into(), port))
        .await
        .unwrap();
    t06_speed_daemon::server(listener).await;
    return;

    let state = State::new();

    for client_id in 0.. {
        let (tcp_stream, socket_addr) = select! {
            conn = listener.accept() => conn.unwrap(),
            _ = signal::ctrl_c() => break
        };
        info!("Connected to client {client_id} at {socket_addr}");

        let shutdown_token = shutdown.token();
        let state = state.clone();

        spawn(async move {
            match t03_budget_chat::handle_connection(tcp_stream, shutdown_token, client_id, state)
                .await
            {
                Ok(_) => info!("handler finished for client {client_id}"),
                Err(e) => warn!("handler error for client {client_id}: {e}"),
            }
        });
    }

    info!("shutting down");
    shutdown.shutdown().await;
}
