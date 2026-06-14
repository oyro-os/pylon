use clap::{Parser, ValueEnum};

#[derive(Parser, Debug, Clone)]
#[command(name = "pylon-load", about = "Pusher-protocol load-test harness")]
pub struct Cli {
    /// WS URL, e.g. ws://127.0.0.1:7000/app/app-key
    #[arg(long, default_value = "ws://127.0.0.1:7000/app/app-key")]
    pub url: String,
    /// Second WS URL for the cluster scenario (node B)
    #[arg(long)]
    pub url_b: Option<String>,
    /// REST base, e.g. http://127.0.0.1:7000
    #[arg(long, default_value = "http://127.0.0.1:7000")]
    pub rest: String,
    #[arg(long, default_value = "app")]
    pub app_id: String,
    #[arg(long, default_value = "app-key")]
    pub key: String,
    #[arg(long, default_value = "app-secret")]
    pub secret: String,
    #[arg(long, value_enum, default_value_t = Scenario::Fanout)]
    pub scenario: Scenario,
    /// Number of connections
    #[arg(long, default_value_t = 1000)]
    pub conns: usize,
    /// Number of channels (channels scenario)
    #[arg(long, default_value_t = 1)]
    pub channels: usize,
    /// Publish rate (events/sec)
    #[arg(long, default_value_t = 10)]
    pub rate: u64,
    /// Measured duration (seconds)
    #[arg(long, default_value_t = 10)]
    pub secs: u64,
    /// Connection ramp (new conns/sec; 0 = all at once)
    #[arg(long, default_value_t = 2000)]
    pub ramp_per_sec: usize,
    /// Private channels (sign the subscribe)
    #[arg(long, default_value_t = false)]
    pub private: bool,
    /// Server PID to sample CPU/RSS (optional)
    #[arg(long)]
    pub server_pid: Option<u32>,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    Connect,
    Fanout,
    Channels,
    Cluster,
}
