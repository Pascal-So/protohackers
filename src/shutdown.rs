pub struct ShutdownToken {
    task_done_sender: tokio::sync::mpsc::Sender<()>,
    should_stop_receiver: tokio::sync::broadcast::Receiver<()>,
}

impl ShutdownToken {
    pub async fn wait_for_shutdown(&mut self) {
        self.should_stop_receiver.recv().await.ok();
    }
}

pub struct ShutdownController {
    tasks_done_receiver: tokio::sync::mpsc::Receiver<()>,
    tasks_done_sender: tokio::sync::mpsc::Sender<()>,
    should_stop_sender: tokio::sync::broadcast::Sender<()>,
}

impl ShutdownController {
    pub fn new() -> Self {
        let (ms, mr) = tokio::sync::mpsc::channel(1);
        let (bs, _) = tokio::sync::broadcast::channel(1);

        ShutdownController {
            tasks_done_receiver: mr,
            tasks_done_sender: ms,
            should_stop_sender: bs,
        }
    }

    pub fn token(&self) -> ShutdownToken {
        ShutdownToken {
            task_done_sender: self.tasks_done_sender.clone(),
            should_stop_receiver: self.should_stop_sender.subscribe(),
        }
    }

    pub async fn shutdown(mut self) {
        self.should_stop_sender.send(()).ok();

        drop(self.tasks_done_sender);
        self.tasks_done_receiver.recv().await;
    }
}
