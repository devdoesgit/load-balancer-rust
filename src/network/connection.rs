use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::listener::handle_connection;

pub async fn run(cancellation_token: CancellationToken) -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?; // fatal: fine to return

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
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream).await {
                        eprintln!("connection error from {peer}: {e}");
                    }
                });
            }
        }
    }

    Ok(())
}