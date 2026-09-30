/*
    Module to load active config from config.toml
        - Parse the config
        - Load them in BackendPool
*/
use std::net::SocketAddr;
use serde::Deserialize;
use std::fs;
use std::fmt;

#[derive(Deserialize)]
pub struct BackendConfig {
    pub remote_address: SocketAddr,
}

#[derive(Deserialize)]
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

impl Config {
    pub fn load() -> Result<Config, anyhow::Error> {
        let contents = fs::read_to_string("./src/config/config.toml")?;
        let config: Config = toml::from_str(&contents)?;

        Ok(config)
    }
}