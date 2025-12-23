use async_runtime::{spawn, Runtime, TcpListener};
use std::io;

fn main() -> io::Result<()> {
    println!("=== HTTP Server Example ===");
    println!("Starting HTTP server on http://127.0.0.1:8080");
    println!("Test with: curl http://localhost:8080\n");

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
                        let mut buf = vec![0u8; 4096];

                        // Read the HTTP request
                        match stream.read(&mut buf).await {
                            Ok(n) if n > 0 => {
                                let request = String::from_utf8_lossy(&buf[..n]);
                                println!("Request:\n{}", request.lines().next().unwrap_or(""));

                                // Simple HTTP response
                                let response = concat!(
                                    "HTTP/1.1 200 OK\r\n",
                                    "Content-Type: text/html; charset=utf-8\r\n",
                                    "Content-Length: 87\r\n",
                                    "\r\n",
                                    "<html><body>",
                                    "<h1>Hello from async-runtime!</h1>",
                                    "<p>Built from scratch</p>",
                                    "</body></html>"
                                );

                                if let Err(e) = stream.write(response.as_bytes()).await {
                                    eprintln!("Write error: {}", e);
                                }
                            }
                            Ok(_) => {
                                println!("Empty request from {}", addr);
                            }
                            Err(e) => {
                                eprintln!("Read error: {}", e);
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
