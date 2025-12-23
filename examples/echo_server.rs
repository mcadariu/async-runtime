use async_runtime::{spawn, Runtime, TcpListener};
use std::io;

fn main() -> io::Result<()> {
    println!("=== Echo Server Example ===");
    println!("Starting echo server on 127.0.0.1:8080");
    println!("Test with: echo 'Hello' | nc localhost 8080\n");

    let rt = Runtime::new();

    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:8080".parse().unwrap())?;
        println!("Server listening on {}", listener.local_addr()?);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("Accepted connection from {}", addr);

                    // Spawn a task to handle this connection
                    spawn(async move {
                        let mut buf = vec![0u8; 1024];

                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) => {
                                    println!("Connection closed by {}", addr);
                                    break;
                                }
                                Ok(n) => {
                                    println!("Received {} bytes from {}", n, addr);

                                    // Echo back
                                    if let Err(e) = stream.write(&buf[..n]).await {
                                        eprintln!("Write error: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Read error: {}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }
    })
}
