mod backend;
mod config;
mod network;
mod health;

use config::Config;
use backend::backend_pool::BackendPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // load configs
    let configs = match Config::load() {
        Ok(c) => {
            println!("Config Loaded...");
            println!("{}", c);
            c
        },
        Err(e) => {
            // return immediately
            return Err(e);
        }
    };

    // load active backend modules (shared: health checker writes flags, network will read them)
    let backend_pool = Arc::new(BackendPool::new(&configs));

    // create cancellation token
    let token = CancellationToken::new();
    let health_token = token.clone();
    let health_pool = Arc::clone(&backend_pool);
    let health_settings = configs.health.clone();

    // start health-checker under its supervisor (restarts it on panic, gives up after a few tries)
    let mut health = tokio::spawn(health::health_checker::supervise(
        health_token,
        health_pool,
        health_settings,
    ));
    let mut health_done = false;

    // tracks spawned connection tasks so they can be drained on shutdown
    let tracker = TaskTracker::new();

    // systemd / docker stop send SIGTERM, not SIGINT
    let mut sigterm = signal(SignalKind::terminate())?;

    let server_result = tokio::select! {
        res = network::connection::run(configs.listen_address, token.clone(), tracker.clone()) => {
            // report the real cause first, before the shutdown logs bury it
            if let Err(e) = &res {
                eprintln!("\nListener failed on {}: {e:#}", configs.listen_address);
            }
            res
        }
        // backstop: supervisor gave up (or died) -> fail loudly, systemd restarts the process
        joined = &mut health => {
            health_done = true;
            let err = match joined {
                Ok(Ok(())) => anyhow::anyhow!("health supervisor stopped unexpectedly"),
                Ok(Err(e)) => e,
                Err(e) => anyhow::anyhow!("health supervisor panicked: {e}"),
            };
            eprintln!("\n{err:#}");
            Err(err)
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nReceived SIGINT");
            Ok(())
        }
        _ = sigterm.recv() => {
            println!("\nReceived SIGTERM");
            Ok(())
        }
    };

    println!("!----- Shutting Down Load Balancer -----!\n");

    // stop accepting + stop health checker
    token.cancel();

    // drain in-flight connections (bounded), second signal forces exit
    tracker.close();
    tokio::select! {
        _ = tracker.wait() => println!("All connections drained"),
        _ = tokio::time::sleep(DRAIN_TIMEOUT) => {
            eprintln!("Drain timeout ({}s) hit, forcing shutdown with {} open connection(s)", DRAIN_TIMEOUT.as_secs(), tracker.len());
        }
        _ = tokio::signal::ctrl_c() => eprintln!("Second SIGINT, forcing shutdown"),
        _ = sigterm.recv() => eprintln!("Second SIGTERM, forcing shutdown"),
    }

    // skip if the select! already collected it: awaiting a finished JoinHandle again panics
    if !health_done {
        health.await??;
        println!("!----- Shutting Down Health Checker -----!");
    }

    server_result?;
    Ok(())
}