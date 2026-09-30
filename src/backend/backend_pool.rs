/*
    Module to store instances of multiple backend modules
*/
use super::backend::Backend;

pub struct BackendPool {
    pub backend_pool: Vec<Backend>
}

impl BackendPool {
    pub fn new(pool: Vec<Backend>)-> Self {
        Self {
            backend_pool: pool
        }
    }
}

