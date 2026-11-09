/*
    Service that polls all our backend instances
        - TCP check (L4) -> HTTP check later for L7
        - flips each backend's healthy flag; the pool itself never changes
        - a backend only flips after `failure_threshold` / `success_threshold`
          consecutive results, so one dropped probe doesn't flap it
*/
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

// supervisor restart policy
const MAX_RESTARTS: usize = 5; // restarts allowed within any RESTART_WINDOW span before giving up
const RESTART_WINDOW: Duration = Duration::from_secs(300);
const RESTART_DELAY: Duration = Duration::from_secs(10);

/*
    Keeps the health checker alive without restarting the whole process:
        - runs start() as its OWN task, so a panic stops at that task's boundary
          and comes back to us as a JoinError value we can match on
        - restarts it after RESTART_DELAY if it panics or stops unexpectedly
        - gives up (returns Err) once more than MAX_RESTARTS failures fall inside
          any RESTART_WINDOW span (sliding window), main then fails loudly and
          systemd restarts the process
*/
pub async fn supervise(
    cancellation_token: CancellationToken,
    pool: Arc<BackendPool>,
    settings: HealthConfig,
) -> anyhow::Result<()> {
    // timestamps of recent failures, oldest first (never holds more than MAX_RESTARTS + 1)
    let mut failures: VecDeque<Instant> = VecDeque::new();

    loop {
        let handle = tokio::spawn(start(
            cancellation_token.clone(),
            Arc::clone(&pool),
            settings.clone(),
        ));

        match handle.await {
            Ok(()) if cancellation_token.is_cancelled() => return Ok(()), // normal shutdown
            Ok(()) => eprintln!("[health] checker stopped unexpectedly"),
            Err(e) => eprintln!("[health] checker panicked: {e}"),
        }

        if over_restart_budget(&mut failures, Instant::now()) {
            anyhow::bail!(
                "health checker failed {} times within {}s, giving up",
                failures.len(),
                RESTART_WINDOW.as_secs()
            );
        }
        eprintln!(
            "[health] restarting in {}s (attempt {}/{MAX_RESTARTS})",
            RESTART_DELAY.as_secs(),
            failures.len()
        );

        // wait before restarting, but don't block shutdown
        tokio::select! {
            _ = cancellation_token.cancelled() => return Ok(()),
            _ = tokio::time::sleep(RESTART_DELAY) => {}
        }
    }
}

// a sliding window technique to replace old failure restart 
// records a failure at `now`, forgets failures older than RESTART_WINDOW,
// and reports whether the remaining ones exceed the restart budget
fn over_restart_budget(failures: &mut VecDeque<Instant>, now: Instant) -> bool {
    failures.push_back(now);
    while failures
        .front()
        .is_some_and(|t| now.duration_since(*t) > RESTART_WINDOW)
    {
        failures.pop_front();
    }
    failures.len() > MAX_RESTARTS
}

// the checker itself: runs until cancelled
async fn start(
    cancellation_token: CancellationToken,
    pool: Arc<BackendPool>,
    settings: HealthConfig,
) {
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

#[cfg(test)]
mod tests {
    use super::*;

    // feeds failures at the given offsets (seconds from a base instant),
    // returns the 1-based index of the failure that triggered give-up, if any
    fn gives_up_at(offsets: &[u64]) -> Option<usize> {
        let base = Instant::now();
        let mut failures = VecDeque::new();
        offsets.iter().position(|&s| {
            over_restart_budget(&mut failures, base + Duration::from_secs(s))
        }).map(|i| i + 1)
    }

    #[test]
    fn gives_up_on_sixth_failure_inside_window() {
        assert_eq!(gives_up_at(&[0, 10, 20, 30, 40, 50]), Some(6));
    }

    #[test]
    fn burst_across_old_window_boundary_still_gives_up() {
        // the fixed-window version reset at 305s and never gave up here
        assert_eq!(gives_up_at(&[240, 250, 260, 270, 280, 305]), Some(6));
    }

    #[test]
    fn spread_out_failures_never_give_up() {
        // one failure every 61s: never more than 5 inside any 300s span
        let offsets: Vec<u64> = (0..20).map(|i| i * 61).collect();
        assert_eq!(gives_up_at(&offsets), None);
    }

    #[test]
    fn old_failures_age_out() {
        // 5 quick failures, then quiet for >300s, then 5 more: still within budget
        assert_eq!(gives_up_at(&[0, 10, 20, 30, 40, 400, 410, 420, 430, 440]), None);
    }
}
