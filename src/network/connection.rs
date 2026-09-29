use std::io;
use tokio::net::TcpListener;

use super::listener::handle_connection;

pub async fn run() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        println!("Connected to {}", stream.peer_addr()?);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}