use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::listener::handle_connection;

pub async fn run(
    listen_address: SocketAddr,
    cancellation_token: CancellationToken,
    tracker: TaskTracker,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_address).await?; // fatal: fine to return
    println!("Listening on {listen_address}");

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(conn) => conn,
                    Err(e) => {
                        eprintln!("accept error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                println!("Connected to {peer}");
                // tracked so shutdown can drain in-flight connections
                tracker.spawn(async move {
                    if let Err(e) = handle_connection(stream).await {
                        eprintln!("connection error from {peer}: {e}");
                    }
                });
            }
        }
    }

    Ok(())
}
