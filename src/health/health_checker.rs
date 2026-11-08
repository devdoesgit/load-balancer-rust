/*
    Service that polls all our backend instances
        - TCP check (L4) -> HTTP check later for L7
        - flips each backend's healthy flag; the pool itself never changes
        - a backend only flips after `failure_threshold` / `success_threshold`
          consecutive results, so one dropped probe doesn't flap it
*/
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::backend::backend_pool::BackendPool;
use crate::config::HealthConfig;

// consecutive probe results for one backend, owned only by the health checker
#[derive(Default, Clone, Copy)]
struct ProbeStreak {
    successes: u32,
    failures: u32,
}

pub async fn start(
    cancellation_token: CancellationToken,
    pool: Arc<BackendPool>,
    settings: HealthConfig,
) -> std::io::Result<()> {
    let interval = Duration::from_secs(settings.interval_seconds.max(1));
    let probe_timeout = Duration::from_secs(settings.timeout_seconds.max(1));

    let mut ticker = tokio::time::interval(interval);
    // if a round runs long, don't fire a burst of catch-up ticks
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut streaks = vec![ProbeStreak::default(); pool.backend_pool.len()];

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => break,
            _ = ticker.tick() => {
                // a round is bounded by probe_timeout, but don't hold up shutdown for it
                tokio::select! {
                    _ = cancellation_token.cancelled() => break,
                    _ = poll_backend_service(&pool, &mut streaks, &settings, probe_timeout) => {}
                }
            }
        }
    }
    Ok(())
}

// probes every backend concurrently and applies the threshold logic
async fn poll_backend_service(
    pool: &BackendPool,
    streaks: &mut [ProbeStreak],
    settings: &HealthConfig,
    probe_timeout: Duration,
) {
    /* --- refer to my high level design for flow of this fn --- */

    // concurrent probes: one dead backend timing out shouldn't delay the rest
    let mut probes = JoinSet::new();
    for (idx, backend) in pool.backend_pool.iter().enumerate() {
        let addr = backend.address;
        probes.spawn(async move { (idx, tcp_probe(addr, probe_timeout).await) });
    }

    while let Some(joined) = probes.join_next().await {
        let Ok((idx, probe_ok)) = joined else { continue }; // probe task panicked: skip it
        let backend = &pool.backend_pool[idx];
        let streak = &mut streaks[idx];

        if probe_ok {
            streak.successes = streak.successes.saturating_add(1);
            streak.failures = 0;
        } else {
            streak.failures = streak.failures.saturating_add(1);
            streak.successes = 0;
        }

        let currently_healthy = backend.is_healthy();
        if !currently_healthy && streak.successes >= settings.success_threshold {
            backend.set_status(true);
            println!("[health] {} is UP ({} consecutive successes)", backend.address, streak.successes);
        } else if currently_healthy && streak.failures >= settings.failure_threshold {
            backend.set_status(false);
            println!("[health] {} is DOWN ({} consecutive failures)", backend.address, streak.failures);
        }
    }
}

// healthy = TCP handshake completes within the timeout
async fn tcp_probe(addr: std::net::SocketAddr, probe_timeout: Duration) -> bool {
    // simple match that only returns true if conneciton is establihed within timeout seconds
    matches!(
        tokio::time::timeout(probe_timeout, TcpStream::connect(addr)).await, Ok(Ok(_))
    )
}
