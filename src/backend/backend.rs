/*
    Module for single instance of a backend Module
*/
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Backend {
    pub address: SocketAddr,
    healthy: AtomicBool
}

impl Backend {
    pub fn new(addr: SocketAddr, healthy: bool) -> Self {
        Self {
            address: addr,
            healthy: AtomicBool::new(healthy)
        }
    }

    // return the last_status of backend instance
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    // change running status of backend instance
    pub fn set_status(&self, healthy: bool) {
        self.healthy.swap(healthy, Ordering::Relaxed) != healthy;
    }
}