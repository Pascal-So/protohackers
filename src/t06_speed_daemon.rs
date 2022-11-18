use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex as SMutex},
    time::Duration,
};

use deku::{
    bitvec::{BitSlice, BitVec, Msb0},
    ctx::Endian,
    prelude::*,
};
use log::{debug, info, warn};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
    spawn,
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex as TMutex,
    },
    time::sleep,
};

#[derive(PartialEq, Eq, Clone, Hash, DekuRead, DekuWrite)]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
struct Str(
    #[deku(
        writer = "write_arr(deku::output, &self.0)",
        reader = "read_arr(deku::rest)"
    )]
    Vec<u8>,
);

impl std::fmt::Debug for Str {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        String::from_utf8_lossy(&self.0).fmt(f)
    }
}

impl Str {
    fn from_str(s: &str) -> Self {
        Self(s.as_bytes().into())
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
struct ClientId(i32);

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, PartialEq, Eq, DekuWrite)]
#[deku(type = "u8", bytes = "1", endian = "big")]
enum ServerMessage {
    #[deku(id = "0x10")]
    Error { msg: Str },

    #[deku(id = "0x21")]
    Ticket(Ticket),

    #[deku(id = "0x41")]
    Heartbeat,
}

#[derive(Debug, PartialEq, Eq, DekuRead)]
#[deku(endian = "big", type = "u8", bytes = "1")]
enum ClientMessage {
    #[deku(id = "0x20")]
    Plate { plate: Str, timestamp: u32 },

    #[deku(id = "0x40")]
    WantHeartbeat {
        /// deciseconds
        interval: u32,
    },

    #[deku(id = "0x80")]
    IAmCamera(CameraData),

    #[deku(id = "0x81")]
    IAmDispatcher {
        #[deku(reader = "read_arr(deku::rest)")]
        roads: Vec<u16>,
    },
}

#[derive(Debug)]
enum ClientReadResult {
    Message(ClientMessage),
    Disconnected,
    InvalidMessageType,
}

fn read_arr<'a, T: DekuRead<'a, Endian>>(
    rest: &'a BitSlice<Msb0, u8>,
) -> Result<(&'a BitSlice<Msb0, u8>, Vec<T>), DekuError> {
    let (mut rest, len) = u8::read(rest, ())?;
    let mut out = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let read = T::read(rest, Endian::Big)?;
        rest = read.0;
        out.push(read.1);
    }
    Ok((rest, out))
}

fn write_arr<T: DekuWrite>(output: &mut BitVec<Msb0, u8>, arr: &[T]) -> Result<(), DekuError> {
    let len = arr.len() as u8;
    len.write(output, ())?;

    for v in arr {
        v.write(output, ())?;
    }
    Ok(())
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, DekuRead)]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
struct CameraData {
    road: u16,
    mile: u16,
    /// miles per hour
    limit: u16,
}

#[derive(Debug, PartialEq, Eq)]
enum ClientType {
    Camera,
    Dispatcher,
    Unknown,
}

#[derive(Debug)]
struct ClientData {
    client_type: ClientType,
    requested_heartbeat: bool,
}

impl ClientData {
    fn new() -> Self {
        ClientData {
            client_type: ClientType::Unknown,
            requested_heartbeat: false,
        }
    }

    /// Returns an error string if the message violates a rule.
    ///
    /// The information about the client is also updated according to the message.
    fn validate_msg(&mut self, msg: &ClientMessage) -> Option<Str> {
        match msg {
            ClientMessage::Plate { .. } => match self.client_type {
                ClientType::Camera { .. } => None,
                _ => Some("not registered as camera"),
            },
            ClientMessage::WantHeartbeat { .. } => {
                if self.requested_heartbeat {
                    Some("already requested heartbeat")
                } else {
                    self.requested_heartbeat = true;
                    None
                }
            }
            ClientMessage::IAmCamera(_) => match self.client_type {
                ClientType::Camera => Some("repeated IAmCamera"),
                ClientType::Dispatcher => Some("IAmCamera after IAmDispatcher"),
                ClientType::Unknown => {
                    self.client_type = ClientType::Camera;
                    None
                }
            },
            ClientMessage::IAmDispatcher { .. } => match self.client_type {
                ClientType::Camera => Some("IAmDispatcher after IAmCamera"),
                ClientType::Dispatcher => Some("repeated IAmDispatcher"),
                ClientType::Unknown => {
                    self.client_type = ClientType::Dispatcher;
                    None
                }
            },
        }
        .map(Str::from_str)
    }
}

struct ClientReader {
    buf: [u8; 520],
    buf_filled: usize,
    stream_read: OwnedReadHalf,
}

impl ClientReader {
    fn new(stream_read: OwnedReadHalf) -> Self {
        Self {
            buf: [0; 520],
            buf_filled: 0,
            stream_read,
        }
    }

    async fn read_message(&mut self) -> std::io::Result<ClientReadResult> {
        loop {
            match ClientMessage::from_bytes((&self.buf[..self.buf_filled], 0)) {
                Ok(((remaining, _), msg)) => {
                    let msg_len = self.buf_filled - remaining.len();
                    self.buf.rotate_left(msg_len);
                    self.buf_filled -= msg_len;

                    return Ok(ClientReadResult::Message(msg));
                }
                Err(DekuError::Incomplete(_)) => {
                    assert!(self.buf_filled < self.buf.len());
                    let bytes = self
                        .stream_read
                        .read(&mut self.buf[self.buf_filled..])
                        .await?;
                    if bytes == 0 {
                        return Ok(ClientReadResult::Disconnected);
                    }

                    self.buf_filled += bytes;
                }
                Err(DekuError::Parse(_)) => return Ok(ClientReadResult::InvalidMessageType),
                Err(e) => {
                    warn!("Unexpected deku error {e}");
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                }
            }
        }
    }
}

struct ClientWriter(OwnedWriteHalf);

impl ClientWriter {
    async fn send_message(&mut self, msg: &ServerMessage) -> std::io::Result<()> {
        self.0.write_all(&msg.to_bytes().unwrap()).await
    }
}

#[derive(PartialEq, Eq, Debug, Clone, DekuWrite)]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
struct Ticket {
    plate: Str,
    road: u16,
    mile1: u16,
    timestamp1: u32,
    mile2: u16,
    timestamp2: u32,
    /// Centimiles per hour
    speed: u16,
}

struct Observations {
    /// (car, road) -> timestamp -> mile
    obs: HashMap<(Str, u16), BTreeMap<u32, u16>>,
}

/// Get the speed in centimiles per hour for two `(timestamp, mile)`
/// observations on the same road.
fn calculate_speed(a: (u32, u16), b: (u32, u16)) -> u16 {
    let dx = b.1.abs_diff(a.1);
    let dt = b.0.abs_diff(a.0);

    (dx as u32 * 100 * 3600 / dt) as u16
}

impl Observations {
    pub fn new() -> Self {
        Self {
            obs: HashMap::new(),
        }
    }

    /// Add a new observation.
    ///
    /// This doesn't check the "1 ticket per day" rule yet.
    pub fn insert(
        &mut self,
        plate: Str,
        timestamp: u32,
        camera_data: CameraData,
    ) -> (Option<Ticket>, Option<Ticket>) {
        match self.obs.entry((plate.clone(), camera_data.road)) {
            Entry::Occupied(mut entry) => {
                let ts = entry.get_mut();

                let prev = ts.range(..timestamp).next_back();
                let next = ts.range(timestamp..).next();

                let maybe_ticket = |a: (u32, u16), b: (u32, u16)| {
                    let speed = calculate_speed(a, b);
                    if speed >= camera_data.limit * 100 + 50 {
                        Some(Ticket {
                            plate: plate.clone(),
                            road: camera_data.road,
                            mile1: a.1,
                            timestamp1: a.0,
                            mile2: b.1,
                            timestamp2: b.0,
                            speed,
                        })
                    } else {
                        None
                    }
                };

                let t1 =
                    prev.and_then(|(&t, &x)| maybe_ticket((t, x), (timestamp, camera_data.mile)));
                let t2 =
                    next.and_then(|(&t, &x)| maybe_ticket((timestamp, camera_data.mile), (t, x)));

                ts.insert(timestamp, camera_data.mile);

                (t1, t2)
            }
            Entry::Vacant(entry) => {
                let mut set = BTreeMap::new();
                set.insert(timestamp, camera_data.mile);
                entry.insert(set);

                (None, None)
            }
        }
    }
}

fn day(timestamp: u32) -> u32 {
    timestamp / 86400
}

async fn filter_tickets(mut receiver: Receiver<Ticket>, sender: Sender<Ticket>) {
    let mut ticketed_days = HashMap::new();

    'outer: while let Some(ticket) = receiver.recv().await {
        debug!("filter got ticket {ticket:?}");
        // check if we can send a ticket for these days
        match ticketed_days.entry(ticket.plate.clone()) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                entry.insert(HashSet::new());
            }
        }

        let map: &mut HashSet<u32> = ticketed_days.get_mut(&ticket.plate).unwrap();
        let range = day(ticket.timestamp1)..=day(ticket.timestamp2);

        for d in range.clone() {
            if map.contains(&d) {
                continue 'outer;
            }
        }
        for d in range {
            map.insert(d);
        }

        if sender.send(ticket).await.is_err() {
            break;
        }
    }
}

enum DispatcherEvent {
    Connected(ClientId, Vec<u16>),
    Disconnected(ClientId),
}

async fn dispatch_tickets(
    mut tickets: Receiver<Ticket>,
    mut dispatcher_events: Receiver<DispatcherEvent>,
    sender: Sender<(ClientId, Option<ServerMessage>)>,
) {
    let pending = Arc::new(SMutex::new(Vec::<Ticket>::new()));
    let dispatchers = Arc::new(SMutex::new(HashMap::new()));

    {
        let pending = pending.clone();
        let dispatchers = dispatchers.clone();
        let sender = sender.clone();
        spawn(async move {
            while let Some(evt) = dispatcher_events.recv().await {
                match evt {
                    DispatcherEvent::Connected(client_id, roads) => {
                        dispatchers.lock().unwrap().insert(client_id, roads.clone());

                        let mut retained = vec![];
                        let mut to_send = vec![];
                        {
                            let mut old = pending.lock().unwrap();
                            for ticket in old.drain(..) {
                                if roads.contains(&ticket.road) {
                                    to_send.push(ticket);
                                } else {
                                    retained.push(ticket);
                                }
                            }
                            *old = retained;
                        }

                        for ticket in to_send {
                            sender
                                .send((client_id, Some(ServerMessage::Ticket(ticket))))
                                .await
                                .ok();
                        }
                    }
                    DispatcherEvent::Disconnected(client_id) => {
                        dispatchers.lock().unwrap().remove(&client_id);
                    }
                }
            }
        });
    }

    while let Some(ticket) = tickets.recv().await {
        debug!("dispatcher got ticket {ticket:?}");

        let client_id = dispatchers
            .lock()
            .unwrap()
            .iter()
            .find(|(_, roads)| roads.contains(&ticket.road))
            .map(|(client_id, _)| *client_id);

        match client_id {
            Some(client_id) => {
                sender
                    .send((client_id, Some(ServerMessage::Ticket(ticket))))
                    .await
                    .ok();
            }
            None => {
                debug!("pushing ticket to pending: {ticket:?}");
                pending.lock().unwrap().push(ticket);
            }
        }
    }
}

async fn handle_read_half(
    dispatcher_events: Sender<DispatcherEvent>,
    observations: Arc<SMutex<Observations>>,
    tickets: Sender<Ticket>,
    mut reader: ClientReader,
    client_id: ClientId,
    sender: Sender<(ClientId, Option<ServerMessage>)>,
) -> std::io::Result<()> {
    let mut client_data = ClientData::new();
    let mut camera_data = None;

    loop {
        let err = match reader.read_message().await? {
            ClientReadResult::Message(msg) => {
                debug!("{client_id:>5} -> {msg:?}");

                let err = client_data.validate_msg(&msg);

                if err.is_none() {
                    match msg {
                        ClientMessage::Plate { plate, timestamp } => {
                            let (ticket1, ticket2) = observations.lock().unwrap().insert(
                                plate,
                                timestamp,
                                camera_data.unwrap(),
                            );

                            if let Some(ticket) = ticket1 {
                                tickets.send(ticket).await.ok();
                            }
                            if let Some(ticket) = ticket2 {
                                tickets.send(ticket).await.ok();
                            }
                        }
                        ClientMessage::WantHeartbeat { interval } => {
                            if interval > 0 {
                                let sender = sender.clone();
                                spawn(async move {
                                    while sender
                                        .send((client_id, Some(ServerMessage::Heartbeat)))
                                        .await
                                        .is_ok()
                                    {
                                        sleep(Duration::from_millis(interval as u64 * 100)).await;
                                    }
                                });
                            }
                        }
                        ClientMessage::IAmCamera(cd) => camera_data = Some(cd),
                        ClientMessage::IAmDispatcher { roads } => {
                            dispatcher_events
                                .send(DispatcherEvent::Connected(client_id, roads))
                                .await
                                .ok();
                        }
                    }
                }

                err
            }
            ClientReadResult::Disconnected => {
                info!("client {client_id} disconnected");

                dispatcher_events
                    .send(DispatcherEvent::Disconnected(client_id))
                    .await
                    .ok();
                sender.send((client_id, None)).await.ok();

                break;
            }
            ClientReadResult::InvalidMessageType => Some(Str::from_str("invalid message")),
        };

        if let Some(err) = err {
            let msg = ServerMessage::Error { msg: err };
            sender.send((client_id, Some(msg))).await.ok();
            sender.send((client_id, None)).await.ok();

            break;
        }
    }

    Ok(())
}

async fn sender(
    writers: Arc<TMutex<HashMap<ClientId, ClientWriter>>>,
    mut msgs: Receiver<(ClientId, Option<ServerMessage>)>,
) {
    loop {
        let (client_id, msg) = match msgs.recv().await {
            Some(msg) => msg,
            None => break,
        };

        debug!("{client_id:>5} <- {msg:?}");

        if let Entry::Occupied(mut entry) = writers.lock().await.entry(client_id) {
            if let Some(msg) = msg {
                if let Err(e) = entry.get_mut().send_message(&msg).await {
                    warn!("error while sending message {msg:?} to {client_id}: {e}");
                }
            } else {
                entry.remove();
            }
        }
    }
}

pub async fn server(listener: TcpListener) {
    let observations = Arc::new(SMutex::new(Observations::new()));
    let writers = Arc::new(TMutex::new(HashMap::new()));
    let (tx_to_sender, rx_to_sender) = mpsc::channel(120);
    let (tx_tickets_to_dispatcher, rx_tickets_to_dispatcher) = mpsc::channel(120);
    let (tx_events_to_dispatcher, rx_events_to_dispatcher) = mpsc::channel(120);
    let (tx_to_filter, rx_to_filter) = mpsc::channel(120);

    {
        let writers = writers.clone();
        spawn(sender(writers, rx_to_sender));
    }

    {
        let tx_to_sender = tx_to_sender.clone();
        spawn(dispatch_tickets(
            rx_tickets_to_dispatcher,
            rx_events_to_dispatcher,
            tx_to_sender,
        ));
    }

    spawn(filter_tickets(rx_to_filter, tx_tickets_to_dispatcher));

    for client_id in 0.. {
        if let Ok((tcp_stream, socket_addr)) = listener.accept().await {
            info!("Connected to client {client_id} at {socket_addr}");

            let observations = observations.clone();
            let tx_to_sender = tx_to_sender.clone();
            let tx_to_filter = tx_to_filter.clone();
            let tx_events_to_dispatcher = tx_events_to_dispatcher.clone();
            let writers = writers.clone();

            spawn(async move {
                let client_id = ClientId(client_id);

                let (read, write) = tcp_stream.into_split();
                writers.lock().await.insert(client_id, ClientWriter(write));

                match handle_read_half(
                    tx_events_to_dispatcher,
                    observations,
                    tx_to_filter,
                    ClientReader::new(read),
                    client_id,
                    tx_to_sender,
                )
                .await
                {
                    Ok(_) => info!("handler finished for client {client_id}"),
                    Err(e) => warn!("handler error for client {client_id}: {e}"),
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(bytes: &[u8]) -> ClientMessage {
        ClientMessage::from_bytes((bytes, 0)).unwrap().1
    }

    #[test]
    fn test_parse_dispatcher() {
        let msg = parse(&[0x81, 0x03, 0x00, 0x42, 0x01, 0x70, 0x13, 0x88]);

        assert_eq!(
            msg,
            ClientMessage::IAmDispatcher {
                roads: vec![66, 368, 5000]
            }
        );
    }

    #[test]
    fn test_parse_camera() {
        let msg = parse(&[0x80, 0x00, 0x7b, 0x00, 0x08, 0x00, 0x3c]);

        assert_eq!(
            msg,
            ClientMessage::IAmCamera(CameraData {
                road: 123,
                mile: 8,
                limit: 60
            })
        );
    }

    #[test]
    fn test_serialize_ticket() {
        let ticket = Ticket {
            plate: Str::from_str("RE05BKG"),
            road: 368,
            mile1: 1234,
            timestamp1: 1000000,
            mile2: 1235,
            timestamp2: 1000060,
            speed: 6000,
        };

        assert_eq!(
            ServerMessage::Ticket(ticket).to_bytes().unwrap(),
            vec![
                0x21, 0x07, 0x52, 0x45, 0x30, 0x35, 0x42, 0x4b, 0x47, 0x01, 0x70, 0x04, 0xd2, 0x00,
                0x0f, 0x42, 0x40, 0x04, 0xd3, 0x00, 0x0f, 0x42, 0x7c, 0x17, 0x70
            ]
        );
    }

    #[test]
    fn test_insert() {
        let mut obs = Observations::new();
        let plate = Str::from_str("asdf");

        let ins = obs.insert(
            plate.clone(),
            0,
            CameraData {
                road: 123,
                mile: 8,
                limit: 60,
            },
        );
        assert_eq!(ins, (None, None));

        let ins = obs.insert(
            plate.clone(),
            45,
            CameraData {
                road: 123,
                mile: 9,
                limit: 60,
            },
        );
        assert_eq!(
            ins,
            (
                Some(Ticket {
                    plate,
                    road: 123,
                    mile1: 8,
                    timestamp1: 0,
                    mile2: 9,
                    timestamp2: 45,
                    speed: 8000
                }),
                None
            )
        );
    }

    #[tokio::test]
    async fn test_sameday_filter() {
        let (tx_in, rx_in) = mpsc::channel(5);
        let (tx_out, mut rx_out) = mpsc::channel(5);

        let t1 = Ticket {
            plate: Str::from_str("X578JA"),
            road: 3005,
            mile1: 3249,
            timestamp1: 87111913,
            mile2: 3271,
            timestamp2: 87112501,
            speed: 13469,
        };
        let t2 = Ticket {
            plate: Str::from_str("X578JA"),
            road: 3005,
            mile1: 3249,
            timestamp1: 87111913,
            mile2: 3260,
            timestamp2: 87112205,
            speed: 13561,
        };

        spawn(filter_tickets(rx_in, tx_out));

        tx_in.send(t1.clone()).await.unwrap();
        tx_in.send(t2).await.unwrap();
        drop(tx_in);

        assert_eq!(rx_out.recv().await.unwrap(), t1);
        assert!(rx_out.recv().await.is_none());
    }
}
