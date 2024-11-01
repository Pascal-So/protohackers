use serde_json::{json, Value};

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Ok,
    Id {
        id: i64,
    },
    Job {
        id: i64,
        job: Value,
        priority: i64,
        queue: String,
    },
    NoJob,
}

impl Response {
    pub fn to_json(&self) -> Value {
        match self {
            Response::Ok => json!({"status": "ok"}),
            Response::Id { id } => json!({"status": "ok", "id": id}),
            Response::Job {
                id,
                job,
                priority,
                queue,
            } => {
                json!({"status": "ok", "id": id, "job": job, "pri": priority, "queue": queue})
            }
            Response::NoJob => json!({"status": "no-job"}),
        }
    }

    #[cfg(test)]
    pub fn from_json(val: &Value) -> Option<Self> {
        match val["status"].as_str() {
            Some("ok") => {
                let id = val["id"].as_i64();

                let job = val["job"].clone();
                let job = if job.is_null() { None } else { Some(job) };

                let priority = val["pri"].as_i64();
                let queue = val["queue"].as_str();

                match (id, job, priority, queue) {
                    (Some(id), Some(job), Some(priority), Some(queue)) => Some(Response::Job {
                        id,
                        job,
                        priority,
                        queue: queue.to_string(),
                    }),
                    (Some(id), _, _, _) => Some(Response::Id { id }),
                    _ => Some(Response::Ok),
                }
            }
            Some("no-job") => Some(Response::NoJob),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    Put {
        queue: String,
        job: Value,
        priority: i64,
    },
    Get {
        queues: Vec<String>,
        wait: bool,
    },
    Delete {
        id: i64,
    },
    Abort {
        id: i64,
    },
}

impl Request {
    #[cfg(test)]
    pub fn to_json(&self) -> Value {
        match self {
            Request::Put {
                queue,
                job,
                priority,
            } => {
                json!({"request": "put", "queue": queue, "job": job, "pri": priority})
            }
            Request::Get { queues, wait } => {
                json!({"request": "get", "queues": queues, "wait": wait})
            }
            Request::Delete { id } => json!({"request": "delete", "id": id}),
            Request::Abort { id } => json!({"request": "abort", "id": id}),
        }
    }

    pub fn from_json(val: &Value) -> Option<Self> {
        match val["request"].as_str() {
            Some("put") => {
                let queue = val["queue"].as_str();
                let job = val["job"].clone();
                let job = if job.is_null() { None } else { Some(job) };
                let priority = val["pri"].as_i64();
                if let (Some(queue), Some(job), Some(priority)) = (queue, job, priority) {
                    Some(Request::Put {
                        queue: queue.to_string(),
                        job,
                        priority,
                    })
                } else {
                    None
                }
            }
            Some("get") => {
                let queues = val["queues"].as_array().and_then(|v| {
                    v.iter()
                        .map(|s| s.as_str().map(|s| s.to_string()))
                        .collect::<Option<Vec<String>>>()
                });

                let wait = val["wait"].as_bool().unwrap_or(false);
                if let Some(queues) = queues {
                    Some(Request::Get { queues, wait })
                } else {
                    None
                }
            }
            Some("delete") => {
                let id = val["id"].as_i64();
                if let Some(id) = id {
                    Some(Request::Delete { id })
                } else {
                    None
                }
            }
            Some("abort") => {
                let id = val["id"].as_i64();
                if let Some(id) = id {
                    Some(Request::Abort { id })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}


#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn response_decode_inverts_encode() {
        let responses = [
            Response::Ok,
            Response::Id { id: 22 },
            Response::NoJob,
            Response::Job {
                id: 123,
                job: json!([2, 5]),
                priority: 44,
                queue: "somequeue".to_string(),
            },
        ];

        for response in responses {
            assert_eq!(Response::from_json(&response.to_json()).unwrap(), response);
        }
    }

    #[test]
    fn request_decode_inverts_encode() {
        let requests = [
            Request::Put {
                queue: "somequeue".to_string(),
                job: json!({"field": "value"}),
                priority: 11,
            },
            Request::Get {
                queues: vec!["somequeue".to_string(), "otherqueue".to_string()],
                wait: true,
            },
            Request::Delete { id: 55 },
            Request::Abort { id: 66 },
        ];

        for request in requests {
            assert_eq!(Request::from_json(&request.to_json()).unwrap(), request);
        }
    }

    #[test]
    fn wait_is_optional() {
        let req = Request::from_json(&json!({"request": "get", "queues": ["somequeue"]})).unwrap();
        assert_eq!(req, Request::Get { queues: vec!["somequeue".to_string()], wait: false });
    }

    #[test]
    fn can_parse_example_request() {
        let line = r#"{"job":{"title":"j-PawxMVV8"},"pri":100,"queue":"q-7UruM2pK","request":"put"}"#;
        let req = Request::from_json(&serde_json::from_str(line).unwrap()).unwrap();
        assert_eq!(
            req,
            Request::Put {
                queue: "q-7UruM2pK".to_string(),
                job: json!({"title": "j-PawxMVV8"}),
                priority: 100
            }
        );
    }

    #[test]
    fn can_serialize_example_response() {
        let response = Response::Job {
            id: 0,
            job: json!({"title": "j-ll7ggMCa"}),
            priority: 100,
            queue: "q-W8sZNPn2".to_string(),
        };

        let json = json!({
            "status": "ok",
            "id": 0,
            "job": {"title": "j-ll7ggMCa"},
            "pri": 100,
            "queue": "q-W8sZNPn2",
        });

        assert_eq!(response.to_json(), json);
    }
}
