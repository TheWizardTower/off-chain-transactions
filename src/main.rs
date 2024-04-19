use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
// use tokio::net::TcpStream;
use tokio_stream::StreamExt;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for mini-redis operations.
///
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
pub async fn main() -> Result<()> {
    println!("Hello, world!");

    Ok(())
}
