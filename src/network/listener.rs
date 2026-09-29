/*
    -------------------------------------------    
    ----- Accept Incomming TCP Connection -----
    -------------------------------------------
*/
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub async fn handle_connection(mut stream: TcpStream)-> std::io::Result<()>  {
    stream.write_all(b"A").await?;
    Ok(())
}