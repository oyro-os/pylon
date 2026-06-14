use crate::cli::Cli;
use crate::metrics::{Counters, Latency};
use crate::pusher::{run_client, ClientConfig};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod channels;
pub mod cluster;
pub mod connect;
pub mod fanout;

pub struct Harness {
    pub epoch: Instant,
    pub lat: Arc<Latency>,
    pub counters: Arc<Counters>,
    pub shutdown: Arc<tokio::sync::Notify>,
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Harness {
    pub fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            lat: Arc::new(Latency::default()),
            counters: Arc::new(Counters::default()),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            tasks: Vec::new(),
        }
    }

    /// Spawn `n` subscribers across the given channel assignment fn, ramped.
    pub async fn spawn_clients(
        &mut self,
        cli: &Cli,
        url: &str,
        n: usize,
        channel_of: impl Fn(usize) -> String,
    ) {
        let ramp = if cli.ramp_per_sec == 0 { n } else { cli.ramp_per_sec };
        // Parse client source IPs once. If parsing fails or the list is just the single
        // default 127.0.0.1, keep the OS-default behavior (src_ip = None).
        let ips: Vec<IpAddr> = cli
            .client_ips
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        let use_binding =
            ips.len() == cli.client_ips.len() && !(ips.len() == 1 && cli.client_ips[0] == "127.0.0.1");
        for i in 0..n {
            let src_ip = if use_binding && !ips.is_empty() {
                Some(ips[i % ips.len()])
            } else {
                None
            };
            let cfg = ClientConfig {
                url: url.to_string(),
                key: cli.key.clone(),
                secret: cli.secret.clone(),
                channel: channel_of(i),
                private: cli.private,
                src_ip,
            };
            let (e, l, c, s) = (
                self.epoch,
                self.lat.clone(),
                self.counters.clone(),
                self.shutdown.clone(),
            );
            self.tasks.push(tokio::spawn(async move {
                let _ = run_client(cfg, e, l, c, s).await;
            }));
            if ramp > 0 && (i + 1) % ramp == 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    pub async fn drain(self) {
        self.shutdown.notify_waiters();
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

/// Wait until at least `target` subscribes are acked or `timeout` elapses.
pub async fn wait_subscribed(counters: &Counters, target: u64, timeout: Duration) {
    let start = Instant::now();
    loop {
        if counters.subscribed.load(std::sync::atomic::Ordering::Relaxed) >= target {
            return;
        }
        if start.elapsed() > timeout {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
