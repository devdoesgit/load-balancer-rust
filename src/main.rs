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

    // start health-checker
    let health = tokio::spawn(
        async move{
            loop {
                let res = health::health_checker::start(health_token.clone(), Arc::clone(&health_pool), health_settings.clone()).await;
                match res {
                    Ok(()) => {
                        println!("\n!----- Shutting Down Health Checker -----!");
                        break
                    },
                    Err(e) => eprintln!("Health checker exited with error: {:?}", e)
                }
                // retry after 10 seconds, but don't block shutdown while waiting
                tokio::select! {
                    _ = health_token.cancelled() => {
                        println!("\n!----- Shutting Down Health Checker -----!");
                        break
                    }
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                }
            }
        }
    );

    // tracks spawned connection tasks so they can be drained on shutdown
    let tracker = TaskTracker::new();

    // systemd / docker stop send SIGTERM, not SIGINT
    let mut sigterm = signal(SignalKind::terminate())?;

    let server_result = tokio::select! {
        res = network::connection::run(configs.listen_address, token.clone(), tracker.clone()) => res,
        signal = tokio::signal::ctrl_c() => {
            signal?;
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

    health.await?;

    server_result?;
    Ok(())
}