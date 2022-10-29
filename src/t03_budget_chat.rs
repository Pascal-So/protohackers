use std::{collections::HashSet, sync::Arc};

use log::{debug, info};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    select,
    sync::{broadcast, Mutex},
};

use crate::shutdown::ShutdownToken;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct User(String);

impl std::fmt::Display for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug)]
pub enum Event {
    Join(User),
    Leave(User),
    Message(User, String),
}

pub struct State {
    users: Arc<Mutex<HashSet<User>>>,
    sender: broadcast::Sender<(Event, i32)>,
    receiver: broadcast::Receiver<(Event, i32)>,
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            users: self.users.clone(),
            sender: self.sender.clone(),
            receiver: self.receiver.resubscribe(),
        }
    }
}

impl State {
    pub fn new() -> Self {
        let (sender, receiver) = broadcast::channel(16);
        Self {
            users: Arc::new(Mutex::new(HashSet::new())),
            sender,
            receiver,
        }
    }
    pub fn send_event(&self, evt: Event, id: i32) {
        self.sender
            .send((evt, id))
            .expect("We still hold a receiver to the channel");
    }

    pub async fn recv_event(&mut self) -> (Event, i32) {
        self.receiver
            .recv()
            .await
            .expect("We still hold a sender to the channel")
    }
}

pub async fn handle_connection(
    mut stream: TcpStream,
    mut shutdown_token: ShutdownToken,
    client_id: i32,
    state: State,
) -> std::io::Result<()> {
    let (read, mut write) = stream.split();
    let mut lines = BufReader::new(read).lines();

    write.write_all("username?\n".as_bytes()).await?;

    let user = match lines.next_line().await? {
        Some(user) => User(user),
        None => {
            debug!("User disconnected before sending name.");
            return Ok(());
        }
    };

    if user.0.is_empty() {
        debug!("User tried to sign up with empty name");
        write.write_all("empty name\n".as_bytes()).await?;
        return Ok(());
    }

    if !user.0.bytes().all(|c| c.is_ascii_alphanumeric()) {
        debug!("User tried to sign up with non-alphanumeric name");
        write
            .write_all("non-alphanumeric name\n".as_bytes())
            .await?;
        return Ok(());
    }

    let new_user = state.users.lock().await.insert(user.clone());
    if !new_user {
        debug!("User tried to sign up with duplicate name [{user}]");
        write.write_all("duplicate name\n".as_bytes()).await?;
        return Ok(());
    }

    let mut state = state.clone();

    info!("User {user} joined with id {client_id}");
    {
        let mut msg = String::from("* The chat contains: ");
        let mut count = 0;
        for other in &*state.users.lock().await {
            if *other != user {
                if count > 0 {
                    msg.push_str(", ");
                }
                msg.push_str(&other.0);
                count += 1;
            }
        }
        msg.push('\n');
        debug!("{user:>width$} <- {client_id}: {msg}", width=16);

        write.write_all(msg.as_bytes()).await?;
    }

    state.send_event(Event::Join(user.clone()), client_id);

    loop {
        select! {
            message = lines.next_line() => match message? {
                Some(message) => {
                    debug!("{user:>width$} -> {client_id}: {message}", width=16);
                    state.send_event(Event::Message(user.clone(), message), client_id);
                },
                None => break,
            },
            (evt, origin) = state.recv_event() => {
                if origin != client_id {
                    let msg = match evt {
                        Event::Join(user) => format!("* {user} has entered the chat\n"),
                        Event::Leave(user) => format!("* {user} has left the chat\n"),
                        Event::Message(user, msg) => format!("[{user}] {msg}\n"),
                    };
                    debug!("{user:>width$} <- {client_id}: {msg}", width=16);
                    write.write_all(msg.as_bytes()).await?;

                }
            },
            _ = shutdown_token.wait_for_shutdown() => return Ok(()),
        };
    }

    info!("User [{user}] disconnected");
    state.users.lock().await.remove(&user);
    state.send_event(Event::Leave(user), client_id);

    Ok(())
}
