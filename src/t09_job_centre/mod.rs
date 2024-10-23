mod job_server;
mod messages;
mod tcp;

use thiserror::Error;

use self::job_server::JobServer;
pub use self::tcp::JobCentre;

#[derive(Error, Debug, PartialEq, Eq)]
enum Error {
    #[error("invalid json")]
    InvalidJson,
    #[error("invalid format")]
    InvalidFormat,
    #[error("access denied")]
    AccessDenied,
}

#[cfg(test)]
mod test {
    use std::{
        io::{BufRead, BufReader, Write}, net::{Shutdown, SocketAddr, TcpStream}, sync::atomic::{AtomicBool, Ordering}, time::Duration
    };

    use serde_json::json;

    use self::{
        messages::{Request, Response},
        tcp::JobCentre,
    };
    use crate::server::run_tcp_server;

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn invalid_request() {
        init();

        let addr = run_tcp_server(JobCentre::new(), 0);
        let mut conn = connect(addr);

        conn.write_all(b"invalid\n").unwrap();

        assert!(get_response(&conn).is_err());
    }

    #[test]
    fn example_session() {
        init();

        let addr = run_tcp_server(JobCentre::new(), 0);
        let mut conn = connect(addr);

        let queue = "queue1".to_string();
        let job = json!({"title": "example-job"});
        let priority = 123;

        send_request(
            &mut conn,
            Request::Put {
                queue: queue.clone(),
                job: job.clone(),
                priority,
            },
        );

        let id = match get_response(&conn).unwrap() {
            Response::Id { id } => id,
            other => panic!("expected id, got {other:?}"),
        };

        send_request(
            &mut conn,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn).unwrap() {
            Response::Job {
                id: id2,
                job: job2,
                priority: priority2,
                queue: queue2,
            } => {
                assert_eq!(id, id2);
                assert_eq!(job, job2);
                assert_eq!(queue, queue2);
                assert_eq!(priority, priority2);
            }
            other => panic!("expected job, got {other:?}"),
        }

        send_request(&mut conn, Request::Abort { id });
        match get_response(&conn).unwrap() {
            Response::Ok => {}
            other => panic!("expected ok, got {other:?}"),
        }

        send_request(
            &mut conn,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn).unwrap() {
            Response::Job {
                id: id2,
                job: job2,
                priority: priority2,
                queue: queue2,
            } => {
                assert_eq!(id, id2);
                assert_eq!(job, job2);
                assert_eq!(queue, queue2);
                assert_eq!(priority, priority2);
            }
            other => panic!("expected job, got {other:?}"),
        }

        send_request(&mut conn, Request::Delete { id });
        match get_response(&conn).unwrap() {
            Response::Ok => {}
            other => panic!("expected ok, got {other:?}"),
        }

        send_request(
            &mut conn,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn).unwrap() {
            Response::NoJob => {}
            other => panic!("expected no-job, got {other:?}"),
        }
    }

    #[test]
    fn job_is_aborted_on_disconnect() {
        init();

        let addr = run_tcp_server(JobCentre::new(), 0);
        let mut conn_1 = connect(addr);
        let mut conn_2 = connect(addr);

        let queue = "queue1".to_string();
        let job = json!({"title": "example-job"});
        let priority = 123;

        send_request(
            &mut conn_1,
            Request::Put {
                queue: queue.clone(),
                job: job.clone(),
                priority,
            },
        );
        match get_response(&conn_1).unwrap() {
            Response::Id { .. } => {}
            other => panic!("expected id, got {other:?}"),
        }

        send_request(
            &mut conn_1,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn_1).unwrap() {
            Response::Job { .. } => {}
            other => panic!("expected job, got {other:?}"),
        }

        send_request(
            &mut conn_2,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn_2).unwrap() {
            Response::NoJob => {}
            other => panic!("expected no-job, got {other:?}"),
        }

        conn_1.shutdown(Shutdown::Both).unwrap();

        std::thread::sleep(Duration::from_secs_f32(0.1));

        send_request(
            &mut conn_2,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn_2).unwrap() {
            Response::Job { .. } => {}
            other => panic!("expected job, got {other:?}"),
        }
    }

    #[test]
    fn disconnect_handover() {
        init();

        let addr = run_tcp_server(JobCentre::new(), 0);
        let mut conn_1 = connect(addr);
        let mut conn_2 = connect(addr);

        let queue = "queue1".to_string();
        let job = json!({"title": "example-job"});
        let priority = 123;

        send_request(
            &mut conn_1,
            Request::Put {
                queue: queue.clone(),
                job: job.clone(),
                priority,
            },
        );
        match get_response(&conn_1).unwrap() {
            Response::Id { .. } => {}
            other => panic!("expected id, got {other:?}"),
        }

        send_request(
            &mut conn_1,
            Request::Get {
                queues: vec![queue.clone()],
                wait: false,
            },
        );
        match get_response(&conn_1).unwrap() {
            Response::Job { .. } => {}
            other => panic!("expected job, got {other:?}"),
        }

        let sent_job_request = AtomicBool::new(false);
        let received_job = AtomicBool::new(false);

        std::thread::scope(|s| {
            let handle = s.spawn(|| {
                send_request(
                    &mut conn_2,
                    Request::Get {
                        queues: vec![queue.clone()],
                        wait: false,
                    },
                );
                match get_response(&conn_2).unwrap() {
                    Response::NoJob => {}
                    other => panic!("expected no-job, got {other:?}"),
                }

                send_request(
                    &mut conn_2,
                    Request::Get {
                        queues: vec![queue.clone()],
                        wait: true,
                    },
                );
                sent_job_request.store(true, Ordering::SeqCst);

                match get_response(&conn_2).unwrap() {
                    Response::Job { .. } => {}
                    other => panic!("expected job, got {other:?}"),
                }
                received_job.store(true, Ordering::SeqCst);
            });
            
            while !sent_job_request.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_secs_f32(0.1));
            }
            std::thread::sleep(Duration::from_secs_f32(0.1));
            
            assert_eq!(received_job.load(Ordering::SeqCst), false);
            conn_1.shutdown(Shutdown::Both).unwrap();

            handle.join().unwrap();
            assert_eq!(received_job.load(Ordering::SeqCst), true);
        });
    }

    #[test]
    fn abort_handover() {
        init();

        let addr = run_tcp_server(JobCentre::new(), 0);
        let mut conn_1 = connect(addr);
        let mut conn_2 = connect(addr);

        let queue = "queue1".to_string();
        let job = json!({"title": "example-job"});
        let priority = 123;

        send_request(
            &mut conn_1,
            Request::Put {
                queue: queue.clone(),
                job: job.clone(),
                priority,
            },
        );
        let job_id = match get_response(&conn_1).unwrap() {
            Response::Id { id } => id,
            other => panic!("expected id, got {other:?}"),
        };

        send_request(&mut conn_1, Request::Get { queues: vec![queue.clone()], wait: false });
        match get_response(&conn_1).unwrap() {
            Response::Job { .. } => {}
            other => panic!("expected job, got {other:?}"),
        }

        let sent_job_request = AtomicBool::new(false);
        let received_job = AtomicBool::new(false);

        std::thread::scope(|s| {
            let handle = s.spawn(|| {
                send_request(
                    &mut conn_2,
                    Request::Get {
                        queues: vec![queue.clone()],
                        wait: true,
                    },
                );

                sent_job_request.store(true, Ordering::SeqCst);

                match get_response(&conn_2).unwrap() {
                    Response::Job { .. } => {}
                    other => panic!("expected job, got {other:?}"),
                }

                received_job.store(true, Ordering::SeqCst);
            });

            for _ in 0..10 {
                if sent_job_request.load(Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(Duration::from_secs_f32(0.1));
            }
            assert_eq!(sent_job_request.load(Ordering::SeqCst), true);
            assert_eq!(received_job.load(Ordering::SeqCst), false);

            send_request(&mut conn_1, Request::Abort { id: job_id });
            match get_response(&conn_1).unwrap() {
                Response::Ok => {},
                other => panic!("expected ok, got {other:?}"),
            };

            handle.join().unwrap();
            assert_eq!(received_job.load(Ordering::SeqCst), true);
        });
    }

    fn get_response(conn: &TcpStream) -> Result<Response, ()> {
        let mut reader = BufReader::new(conn);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();

        Response::from_json(&serde_json::from_str(&line).unwrap()).ok_or(())
    }

    fn send_request(conn: &mut TcpStream, request: Request) {
        let request = request.to_json();
        conn.write_all(format!("{request}\n").as_bytes()).unwrap();
    }

    fn connect(addr: SocketAddr) -> TcpStream{
        let conn = std::net::TcpStream::connect(addr).unwrap();
        conn.set_read_timeout(Some(Duration::from_secs_f32(0.5))).unwrap();
        conn
    }
}
