use crate::{MetricResult, MetricSink};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::executor::LocalPool;
use futures::StreamExt;
use std::io;
use std::sync::atomic::AtomicBool;
use std::thread::{self, JoinHandle};

#[derive(Debug)]
pub struct AsyncSink {
    tx: UnboundedSender<String>,
    handle: JoinHandle<()>,
}

impl AsyncSink {
    pub fn from<T>(sink: T) -> MetricResult<Self>
    where
        T: MetricSink + Sync + Send + 'static,
    {
        let (tx, mut rx) = unbounded::<String>();

        let handle = thread::spawn(move || {
            let mut pool = LocalPool::new();
            pool.run_until(async move {
                while let Some(res) = rx.next().await {
                    // This can block since it's the only task running in the local executor
                    let _ = sink.emit(res.as_ref());
                }
            });
        });

        Ok(Self { tx, handle })
    }

    #[cfg(test)]
    fn wait_handle(self) {
        drop(self.tx);
        self.handle.join().unwrap();
    }
}

impl MetricSink for AsyncSink {
    fn emit(&self, metric: &str) -> io::Result<usize> {
        match self.tx.unbounded_send(metric.to_string()) {
            Ok(_) => Ok(metric.len()),
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, err.to_string())),
        }
    }
}

struct Worker {
    rx: UnboundedReceiver<String>,
    stopped: AtomicBool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct TestMetricSink {
        metrics: Arc<Mutex<Vec<String>>>,
    }

    impl TestMetricSink {
        fn new(metrics: Arc<Mutex<Vec<String>>>) -> TestMetricSink {
            TestMetricSink { metrics }
        }
    }

    impl MetricSink for TestMetricSink {
        fn emit(&self, m: &str) -> io::Result<usize> {
            let mut store = self.metrics.lock().unwrap();
            store.push(m.to_string());
            Ok(m.len())
        }
    }

    #[test]
    fn test_sending_metric() {
        let store = Arc::new(Mutex::new(vec![]));
        let wrapped = TestMetricSink::new(store.clone());

        let sink = AsyncSink::from(wrapped).unwrap();
        sink.emit("async_sent:100|c").unwrap();

        sink.wait_handle();
        assert_eq!("async_sent:100|c".to_string(), store.lock().unwrap()[0]);
    }
}
