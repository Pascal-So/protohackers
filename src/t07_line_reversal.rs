use std::{
    collections::{hash_map::Entry::Vacant, HashMap},
    net::SocketAddr,
    str::from_utf8,
    time::Duration,
};

use log::{debug, trace, info};
use tokio::{net::UdpSocket, select, spawn, sync::mpsc, time::Instant};

const SESSION_EXPIRY_TIMEOUT: Duration = Duration::from_secs(60);
const RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_MESSAGE_LEN: usize = 1000;

// use crate::shutdown::ShutdownToken;

#[derive(PartialEq, Eq, Debug, Hash, Clone, Copy)]
struct SessionId(i32);

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

fn slash_escape(input: &str) -> String {
    input.replace('\\', "\\\\").replace('/', "\\/")
}

fn slash_unescape(input: &str) -> String {
    // note: this works because there will be no unescaped slashes in the input
    input.replace("\\/", "/").replace("\\\\", "\\")
}

#[derive(PartialEq, Eq, Debug, Clone)]
enum LRCPMessage {
    Connect,
    Data(usize, String),
    Ack(usize),
    Close,
}

impl LRCPMessage {
    pub fn parse(input: &str) -> Option<(SessionId, LRCPMessage)> {
        let mut escaped = false;
        let mut slash_positions = vec![];
        for (i, c) in input.chars().enumerate() {
            match (c, escaped) {
                ('/', false) => {
                    slash_positions.push(i);
                }
                ('\\', false) => {
                    escaped = true;
                }
                _ => {
                    escaped = false;
                }
            }
        }

        if *slash_positions.first()? != 0
            || *slash_positions.last()? != input.len() - 1
            || slash_positions.len() < 3
        {
            return None;
        }

        let msg_type = &input[1..slash_positions[1]];
        let session_id = SessionId(
            input[slash_positions[1] + 1..slash_positions[2]]
                .parse()
                .ok()?,
        );
        let nr_fields = slash_positions.len() - 3;
        match (msg_type, nr_fields) {
            ("connect", 0) => Some((session_id, LRCPMessage::Connect)),
            ("data", 2) => {
                let pos = input[slash_positions[2] + 1..slash_positions[3]]
                    .parse()
                    .ok()?;
                let data = &input[slash_positions[3] + 1..slash_positions[4]];
                Some((session_id, LRCPMessage::Data(pos, slash_unescape(data))))
            }
            ("ack", 1) => {
                let len = input[slash_positions[2] + 1..slash_positions[3]]
                    .parse()
                    .ok()?;
                Some((session_id, LRCPMessage::Ack(len)))
            }
            ("close", 0) => Some((session_id, LRCPMessage::Close)),
            _ => {
                debug!("    parser: received invalid message type or number of fields: {msg_type} {nr_fields}");
                None
            }
        }
    }

    fn serialize(&self, session_id: SessionId) -> String {
        match self {
            LRCPMessage::Connect => format!("/connect/{session_id}/"),
            LRCPMessage::Data(pos, data) => {
                format!("/data/{session_id}/{pos}/{}/", slash_escape(data))
            }
            LRCPMessage::Ack(len) => format!("/ack/{session_id}/{len}/"),
            LRCPMessage::Close => format!("/close/{session_id}/"),
        }
    }
}

async fn handle_lrcp_session(
    session_id: SessionId,
    mut msg_in: mpsc::Receiver<LRCPMessage>,
    msg_out: mpsc::Sender<(SessionId, LRCPMessage)>,
) -> std::io::Result<()> {
    debug!("    {session_id}: started");

    let mut received_connect = false;
    let mut should_exit = false;

    let mut to_send = "".to_string();
    let mut sending_message = None;
    let mut received_chars = 0;
    let mut received = "".to_string();

    let mut sent_data = "".to_string();
    let mut acked_sent_chars = 0;

    let session_expiry_timeout = tokio::time::sleep(SESSION_EXPIRY_TIMEOUT);
    tokio::pin!(session_expiry_timeout);

    let retransmission_timeout = tokio::time::sleep(Duration::from_secs_f32(0.));
    tokio::pin!(retransmission_timeout);
    retransmission_timeout.as_mut().await;

    let max_data_len = MAX_MESSAGE_LEN
        - LRCPMessage::Data(i32::MAX as usize, String::new())
            .serialize(session_id)
            .len();

    loop {
        trace!("    {session_id}: loop head");

        let response = if sending_message.is_none() && !to_send.is_empty() {
            trace!("    {session_id}: sending out data: {to_send}");

            let (send_now, send_later) = to_send.split_at(max_data_len.min(to_send.len()));
            let data_msg = LRCPMessage::Data(sent_data.len(), send_now.to_string());
            sent_data += send_now;

            to_send = send_later.to_string();

            sending_message = Some(data_msg);
            sending_message.clone()
        } else if sending_message.is_none() && received.contains('\n') {
            // application layer logic lives in here

            let idx = received.find('\n').unwrap();
            let (to_reverse, unprocessed) = received.split_at(idx + 1);
            info!(
                "    {session_id}: reversing string {:?}",
                &to_reverse[..to_reverse.len() - 1]
            );

            let mut to_reverse: Vec<u8> = to_reverse.bytes().collect();
            to_reverse.pop();
            to_reverse.reverse();
            let reversed = String::from_utf8_lossy(&to_reverse).to_string();
            received = unprocessed.to_string();

            to_send = reversed + "\n";

            None
        } else {
            trace!("    {session_id}: selecting");
            select! {
                () = &mut session_expiry_timeout => {
                    debug!("    {session_id}: session timed out");
                    should_exit = true;
                    Some(LRCPMessage::Close)
                }
                () = &mut retransmission_timeout, if sending_message.is_some() => {
                    trace!("    {session_id}: retransmission_timeout");
                    sending_message.clone()
                }
                msg = msg_in.recv() => {
                    let Some(msg) = msg else {break};
                    debug!("    {session_id}: handling message {msg:?}");

                    session_expiry_timeout.as_mut().reset(Instant::now() + SESSION_EXPIRY_TIMEOUT);

                    match msg {
                        LRCPMessage::Connect => {
                            received_connect = true;
                            Some(LRCPMessage::Ack(0))
                        },
                        LRCPMessage::Data(pos, content) => {
                            if !received_connect {
                                should_exit = true;
                                Some(LRCPMessage::Close)
                            } else if pos > received_chars {
                                Some(LRCPMessage::Ack(received_chars))
                            } else {
                                let overlap = received_chars - pos;
                                let fresh = content.len().saturating_sub(overlap);
                                received_chars += fresh;
                                received += &content[overlap.min(content.len())..];
                                Some(LRCPMessage::Ack(received_chars))
                            }
                        },
                        LRCPMessage::Ack(len) => {
                            if len <= acked_sent_chars {
                                None
                            } else if len > sent_data.len() {
                                should_exit = true;
                                debug!("    {session_id}: misbehaving client sent ack {len} but only {} sent", sent_data.len());
                                Some(LRCPMessage::Close)
                            } else if len < sent_data.len() {
                                acked_sent_chars = len;
                                sending_message = Some(LRCPMessage::Data(len, sent_data[len..].to_string()));
                                sending_message.clone()
                            } else {
                                acked_sent_chars = len;
                                sending_message = None;
                                None
                            }
                        },
                        LRCPMessage::Close => {
                            should_exit = true;
                            Some(LRCPMessage::Close)
                        },
                    }
                }
            }
        };
        trace!("    {session_id}: after branching");

        if let Some(response) = response {
            trace!("    {session_id}: sending {response:?}");

            msg_out.send((session_id, response)).await.ok();
            retransmission_timeout
                .as_mut()
                .reset(Instant::now() + RETRANSMISSION_TIMEOUT);
        }

        trace!("    {session_id}: checking should_exit: {should_exit}");
        if should_exit {
            break;
        }
    }
    info!("    {session_id}: handler finished");

    Ok(())
}

pub async fn server(port: u16) -> std::io::Result<()> {
    let mut buf = vec![0u8; MAX_MESSAGE_LEN];

    let mut sessions = HashMap::<SessionId, (SocketAddr, mpsc::Sender<LRCPMessage>)>::new();

    let socket = UdpSocket::bind(SocketAddr::new([0, 0, 0, 0].into(), port)).await?;
    let (tx_out, mut rx_out) = mpsc::channel::<(SessionId, LRCPMessage)>(64);

    loop {
        select! {
            res = socket.recv_from(&mut buf) => {
                let Ok((bytes, origin)) = res else {continue};
                let Ok(msg) = from_utf8(&buf[..bytes]) else {
                    debug!("    server: Received non-utf8 message with {bytes} bytes. Ignoring.");
                    continue;
                };
                debug!("--> {msg}");

                let Some((session_id, msg)) = LRCPMessage::parse(msg) else {
                    debug!("    server: Could not parse message {msg}.");
                    continue;
                };

                if let Vacant(vacant) = sessions.entry(session_id) {
                    let (tx_in, rx_in) = mpsc::channel::<LRCPMessage>(64);
                    info!("    server: Spawning handler for session {session_id}.");
                    spawn(handle_lrcp_session(session_id, rx_in, tx_out.clone()));
                    vacant.insert((origin, tx_in));
                }

                if sessions.get(&session_id).unwrap().1.send(msg).await.is_err() {
                    debug!("    server: Handler for session {session_id} already stopped, can't deliver message.");
                    sessions.remove(&session_id);
                }
            }
            msg = rx_out.recv() => {
                let (session_id, msg) = msg.unwrap();
                let addr = sessions.get(&session_id).unwrap().0;
                let serialized = msg.serialize(session_id);
                debug!("<-- {serialized}");
                if let Err(e) = socket.send_to(serialized.as_bytes(), addr).await {
                    debug!("    server: Could not send message to {addr}: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S123: SessionId = SessionId(123);

    #[test]
    fn test_parse_invalid() {
        assert_eq!(LRCPMessage::parse("a/connect/123/"), None);
        assert_eq!(LRCPMessage::parse("/connect/123/b"), None);
        assert_eq!(LRCPMessage::parse("/con/123/"), None);
        assert_eq!(LRCPMessage::parse("/connect/123/c/"), None);
        assert_eq!(LRCPMessage::parse("/connect/abc/"), None);
    }

    #[test]
    fn test_parse_connect() {
        assert_eq!(
            LRCPMessage::parse("/connect/123/"),
            Some((S123, LRCPMessage::Connect))
        );
    }

    #[test]
    fn test_parse_data() {
        assert_eq!(LRCPMessage::parse("/data/123/abc/abc/"), None);
        assert_eq!(
            LRCPMessage::parse("/data/123/0/abc/"),
            Some((S123, LRCPMessage::Data(0, "abc".to_string())))
        );
    }

    #[test]
    fn test_serialize_data() {
        assert_eq!(
            &LRCPMessage::Data(0, "abc".to_string()).serialize(S123),
            "/data/123/0/abc/"
        );
        assert_eq!(&LRCPMessage::Close.serialize(S123), "/close/123/");
    }

    #[test]
    fn test_unescape() {
        assert_eq!(&slash_unescape("a/b"), "a/b");
        assert_eq!(&slash_unescape("a//b"), "a//b");
        assert_eq!(&slash_unescape("a\\/b"), "a/b");
        assert_eq!(&slash_unescape("a\\\\b"), "a\\b");
    }

    #[test]
    fn test_escape() {
        assert_eq!(&slash_escape("a/b"), "a\\/b");
        assert_eq!(&slash_escape("a//b"), "a\\/\\/b");
        assert_eq!(&slash_escape("a\\b"), "a\\\\b");
    }

    #[tokio::test]
    async fn test_simple_usecase() {
        env_logger::init();

        let (tx_out, mut rx_out) = mpsc::channel::<(SessionId, LRCPMessage)>(64);
        let (tx_in, rx_in) = mpsc::channel::<LRCPMessage>(64);
        let handle = spawn(handle_lrcp_session(S123, rx_in, tx_out.clone()));

        tx_in.send(LRCPMessage::Connect).await.unwrap();
        let msg = rx_out.recv().await.unwrap();
        assert_eq!(msg, (S123, LRCPMessage::Ack(0)));

        tokio::time::sleep(Duration::from_millis(50)).await;

        tx_in
            .send(LRCPMessage::Data(0, "hello\n".to_string()))
            .await
            .unwrap();
        let msg = rx_out.recv().await.unwrap();
        assert_eq!(msg, (S123, LRCPMessage::Ack(6)));

        tokio::time::sleep(Duration::from_millis(50)).await;

        let msg = rx_out.recv().await.unwrap();
        assert_eq!(msg, (S123, LRCPMessage::Data(0, "olleh\n".to_string())));
        tokio::time::sleep(Duration::from_millis(50)).await;

        tx_in.send(LRCPMessage::Close).await.unwrap();
        let msg = rx_out.recv().await.unwrap();
        assert_eq!(msg, (S123, LRCPMessage::Close));

        handle.await.unwrap().unwrap();
    }
}
