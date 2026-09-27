use std::io;
use tokio::net::TcpListener;

use super::listener::handleConnection;

pub async fn run() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = handleConnection(stream).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}