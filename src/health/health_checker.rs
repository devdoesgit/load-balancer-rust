/*
    Service that polls all our backend instances
        - TCP & HTTP Check
        - modifies active backend pool
        -
*/
use tokio_util::sync::CancellationToken;
use std::time::Duration;

pub async fn start(
    cancellation_token: CancellationToken,
    interval: Duration,
) -> std::io::Result<()> {
    let mut ticker = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                break;
            }

            _ = ticker.tick() => {
                poll_backend_service().await;
            }
        }
    }
    Ok(())
}

// this fn polls every backend service and runs health checker core logic
pub async fn poll_backend_service() -> () {
    // fetch backend pool
    // let backend_pool = Vec
}