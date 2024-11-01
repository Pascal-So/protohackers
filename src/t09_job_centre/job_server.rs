use std::sync::{Arc, Condvar, Mutex};

use log::warn;
use serde_json::Value;

use super::{messages::Response, Error};

struct Entry {
    job: Value,
    priority: i64,
    id: i64,
    queue: String,
    deleted: bool,
    client_working: Option<i32>,
}

#[derive(Clone)]
pub struct JobServer {
    queues: Arc<(Mutex<Vec<Entry>>, Condvar)>,
}

impl JobServer {
    pub fn new() -> JobServer {
        let queues = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

        JobServer { queues }
    }

    pub fn put(&self, queue: String, job: Value, priority: i64) -> i64 {
        let (mutex, cvar) = &*self.queues;

        let mut queues = mutex.lock().unwrap();
        let job_id = queues.len() as i64;
        queues.push(Entry {
            job,
            priority,
            id: job_id,
            queue,
            deleted: false,
            client_working: None,
        });

        cvar.notify_all();
        job_id
    }

    pub fn get_nonblocking(&self, queues: &[String], client_id: i32) -> Response {
        let mut entries = self.queues.0.lock().unwrap();

        get_job(&mut entries, queues, client_id)
    }

    pub fn get_blocking(&self, queues: &[String], client_id: i32) -> Response {
        let (mutex, cvar) = &*self.queues;
        let mut entries = mutex.lock().unwrap();

        loop {
            let response = get_job(&mut entries, queues, client_id);
            if let Response::Job { .. } = response {
                return response;
            } else {
                entries = cvar.wait(entries).unwrap();
            }
        }
    }

    pub fn abort(&self, job_id: i64, client_id: i32) -> Result<Response, Error> {
        let (mutex, cvar) = &*self.queues;
        let mut entries = mutex.lock().unwrap();

        if let Some(entry) = entries.iter_mut().find(|e| e.id == job_id && !e.deleted) {
            match entry.client_working {
                Some(cid) if cid == client_id => {
                    entry.client_working = None;
                    cvar.notify_all();

                    Ok(Response::Ok)
                }
                Some(other) => {
                    warn!("client {client_id} tried to abort job {job_id} owned by {other}");
                    Err(Error::AccessDenied)
                }
                None => {
                    warn!("client {client_id} tried to abort idle job {job_id}");
                    Ok(Response::NoJob)
                }
            }
        } else {
            warn!("client {client_id} tried to abort unknown job {job_id}");
            Ok(Response::NoJob)
        }
    }

    pub fn delete(&self, job_id: i64) -> Response {
        self.queues
            .0
            .lock()
            .unwrap()
            .iter_mut()
            .find(|e| e.id == job_id && !e.deleted)
            .map(|e| {
                e.deleted = true;
                Response::Ok
            })
            .unwrap_or(Response::NoJob)
    }

    pub fn client_disconnect(&self, client_id: i32) {
        let (mutex, cvar) = &*self.queues;
        let mut entries = mutex.lock().unwrap();

        for e in entries.iter_mut() {
            if e.client_working == Some(client_id) {
                e.client_working = None;
            }
        }

        cvar.notify_all();
    }
}

fn get_job(entries: &mut Vec<Entry>, queues: &[String], client_id: i32) -> Response {
    let mut candidates: Vec<_> = entries
        .iter_mut()
        .filter(|e| e.client_working.is_none() && !e.deleted && queues.contains(&e.queue))
        .collect::<Vec<_>>();

    candidates.sort_by(|a, b| a.priority.cmp(&b.priority));

    if let Some(entry) = candidates.pop() {
        entry.client_working = Some(client_id);
        Response::Job {
            id: entry.id,
            job: entry.job.clone(),
            priority: entry.priority,
            queue: entry.queue.clone(),
        }
    } else {
        Response::NoJob
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicBool, time::Duration};

    use serde_json::json;

    use super::*;

    #[test]
    fn test_put_get_nonblocking() {
        let server = JobServer::new();

        assert_eq!(
            server.get_nonblocking(&["q1".to_string()], 1),
            Response::NoJob
        );

        let id_1 = server.put("q1".to_string(), json!("job 1"), 1);

        assert_eq!(
            server.get_nonblocking(&["q1".to_string()], 1),
            Response::Job {
                id: id_1,
                job: json!("job 1"),
                priority: 1,
                queue: "q1".to_string()
            }
        );
    }

    #[test]
    fn test_get_blocking() {
        let server = JobServer::new();

        let received: AtomicBool = AtomicBool::new(false);

        std::thread::scope(|s| {
            let handle = s.spawn(|| {
                let _ = server.get_blocking(&["q1".to_string()], 1);
                received.store(true, std::sync::atomic::Ordering::SeqCst);
            });

            std::thread::sleep(Duration::from_secs_f32(0.1));
            assert_eq!(received.load(std::sync::atomic::Ordering::SeqCst), false);

            server.put("q1".to_string(), json!("job 1"), 1);

            std::thread::sleep(Duration::from_secs_f32(0.1));
            assert_eq!(received.load(std::sync::atomic::Ordering::SeqCst), true);

            assert_eq!(handle.join().is_err(), false);
        });
    }

    #[test]
    fn test_get_blocking_multiple() {
        let server = JobServer::new();

        let received_1: AtomicBool = AtomicBool::new(false);
        let received_2: AtomicBool = AtomicBool::new(false);

        std::thread::scope(|s| {
            let handle_1 = s.spawn(|| {
                let _ = server.get_blocking(&["q1".to_string()], 1);
                received_1.store(true, std::sync::atomic::Ordering::SeqCst);
            });

            let handle_2 = s.spawn(|| {
                let _ = server.get_blocking(&["q1".to_string()], 2);
                received_2.store(true, std::sync::atomic::Ordering::SeqCst);
            });

            std::thread::sleep(Duration::from_secs_f32(0.1));
            assert_eq!(received_1.load(std::sync::atomic::Ordering::SeqCst), false);
            assert_eq!(received_2.load(std::sync::atomic::Ordering::SeqCst), false);

            server.put("q1".to_string(), json!("job 1"), 1);

            std::thread::sleep(Duration::from_secs_f32(0.1));
            // only one of the workers should get a job here
            assert_eq!(
                !received_1.load(std::sync::atomic::Ordering::SeqCst),
                received_2.load(std::sync::atomic::Ordering::SeqCst)
            );

            server.put("q1".to_string(), json!("job 2"), 1);

            std::thread::sleep(Duration::from_secs_f32(0.1));
            // both workers have a job
            assert_eq!(received_1.load(std::sync::atomic::Ordering::SeqCst), true);
            assert_eq!(received_2.load(std::sync::atomic::Ordering::SeqCst), true);

            assert_eq!(handle_1.join().is_err(), false);
            assert_eq!(handle_2.join().is_err(), false);
        });
    }

    #[test]
    fn only_active_worker_can_abort() {
        let server = JobServer::new();

        let job_id = server.put("q1".to_string(), json!("job 1"), 1);
        server.get_nonblocking(&["q1".to_string()], 1);

        // others can't get this job
        assert_eq!(
            server.get_nonblocking(&["q1".to_string()], 3),
            Response::NoJob
        );

        let response = server.abort(job_id, 2);
        assert_eq!(response, Err(Error::AccessDenied));

        // job remains unavailable
        assert_eq!(
            server.get_nonblocking(&["q1".to_string()], 3),
            Response::NoJob
        );

        let response = server.abort(job_id, 1);
        assert_eq!(response, Ok(Response::Ok));

        // job is back in the queue
        assert!(matches!(
            server.get_nonblocking(&["q1".to_string()], 3),
            Response::Job { .. }
        ));

        let response = server.abort(job_id + 99, 1);
        assert_eq!(response, Ok(Response::NoJob));
    }

    #[test]
    fn delete_job() {
        let server = JobServer::new();

        assert_eq!(server.delete(123), Response::NoJob);

        let job_id = server.put("q1".to_string(), json!("job 1"), 1);

        assert_eq!(server.delete(job_id), Response::Ok);

        // no more operations should be possible on the deleted job

        assert_eq!(
            server.get_nonblocking(&["q1".to_string()], 1),
            Response::NoJob
        );

        assert_eq!(server.abort(job_id, 1), Ok(Response::NoJob));

        assert_eq!(server.delete(job_id), Response::NoJob);
    }

    #[test]
    fn cannot_abort_idle_job() {
        let server = JobServer::new();

        let job_id = server.put("q1".to_string(), json!("job 1"), 1);
        assert_eq!(server.abort(job_id, 1), Ok(Response::NoJob));
    }
}
