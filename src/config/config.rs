/*
    Module to load active config from config.toml
        - Parse the config
        - Load them in BackendPool
*/
use std::net::SocketAddr;
use anyhow::Context;
use serde::Deserialize;
use std::fs;
use std::fmt;

#[derive(Deserialize)]
pub struct BackendConfig {
    pub remote_address: SocketAddr,
}

#[derive(Deserialize, Clone)]
pub struct HealthConfig {
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    pub failure_threshold: u32,
    pub success_threshold: u32,
}

#[derive(Deserialize)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub backends: Vec<BackendConfig>,
    pub health: HealthConfig
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Listening Address {} \n 
            --- Health settings --- \n 
            Interval Seconds - {} 
            Timeout Seconds - {} 
            Failure Threshold - {} 
            Success Threshold - {} 
            \n
        ", 
        self.listen_address, self.health.timeout_seconds, self.health.interval_seconds, self.health.failure_threshold, self.health.success_threshold)
    }
}

// env var that overrides the config location (set by the systemd unit)
const CONFIG_ENV: &str = "LB_CONFIG";
// fallback for local dev: works when run from the repo root (cargo run)
const DEFAULT_CONFIG_PATH: &str = "./src/config/config.toml";

impl Config {
    pub fn load() -> Result<Config, anyhow::Error> {
        let path = std::env::var(CONFIG_ENV).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

        // context() puts the path in the error, so a bad path is obvious in the logs
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file '{path}' (set {CONFIG_ENV} to override)"))?;
        let config: Config = toml::from_str(&contents)
            .with_context(|| format!("failed to parse config file '{path}'"))?;

        println!("Config loaded from {path}");
        Ok(config)
    }
}