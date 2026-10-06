/*
    Module to store instances of multiple backend modules
*/
use super::backend::Backend;
use crate::config::Config;

pub struct BackendPool {
    pub backend_pool: Vec<Backend>
}

impl BackendPool {
    pub fn new(config: &Config)-> Self {
        Self {
            backend_pool: config.backends.iter().map(|backend_config| {
                Backend::new(
                    backend_config.remote_address,
                    false, // by default, all backends are unhealthy until health check is performed
                )
            }).collect()
        }
    }
}

