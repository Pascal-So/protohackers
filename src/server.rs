use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    thread::spawn,
};

use log::{info, warn};

/// Start a TCP server and run it in the background
pub fn run_tcp_server<Problem: TcpServerProblem>(problem: Problem, port: u16) {
    let listener = TcpListener::bind(SocketAddr::new([0, 0, 0, 0].into(), port)).unwrap();

    spawn(move || {
        for client_id in 0.. {
            let (stream, addr) = listener.accept().unwrap();

            info!("Connected to client {client_id} at {addr}");

            let problem = problem.clone();
            spawn(move || {
                let res = Problem::handle_connection(problem, stream, client_id);
                match res {
                    Ok(()) => info!("Disconnected from client {client_id}"),
                    Err(e) => warn!("Error from client {client_id}: {e}"),
                }
            });
        }
    });
}

pub trait TcpServerProblem: Clone + Send + Sync + 'static {
    fn handle_connection(self, stream: TcpStream, client_id: i32) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;

    #[derive(Clone)]
    struct SimpleProblem(Arc<Mutex<Vec<(String, i32)>>>);

    impl TcpServerProblem for SimpleProblem {
        fn handle_connection(self, mut stream: TcpStream, client_id: i32) -> anyhow::Result<()> {
            let mut buf = String::new();
            stream.read_to_string(&mut buf).unwrap();
            println!("read {buf:?} from client {client_id} and shut down");

            self.0.lock().unwrap().push((buf, client_id));

            Ok(())
        }
    }

    #[test]
    fn test_receives_messages() {
        let problem = SimpleProblem(Arc::new(Mutex::new(vec![])));

        let port = 9515;
        run_tcp_server(problem.clone(), port);

        let mut conn_a = std::net::TcpStream::connect_timeout(
            &SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_secs_f32(0.1),
        )
        .unwrap();

        conn_a.write("hello".as_bytes()).unwrap();
        conn_a.shutdown(std::net::Shutdown::Write).unwrap();
        let mut buf = vec![];
        assert_eq!(conn_a.read_to_end(&mut buf).unwrap(), 0);

        std::net::TcpStream::connect_timeout(
            &SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_secs_f32(0.1),
        )
        .unwrap()
        .write("world".as_bytes())
        .unwrap();

        std::thread::sleep(Duration::from_secs_f32(0.2));

        let state = problem.0.lock().unwrap();
        assert_eq!(state.len(), 2);
        assert_eq!(state[0].0, "hello");
        assert_eq!(state[1].0, "world");

        assert_eq!(state[0].1, 0);
        assert_eq!(state[1].1, 1);
    }
}
