//! Command-line demonstration of the serial runtime.

use ruvoy::{Request, sync::RubyRuntime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RubyRuntime::start_default()?;
    let client = runtime.client();
    let response = client.call(Request::new("POST", "/demo", b"hello".to_vec()))?;

    println!("Ruby: {}", runtime.info().ruby_description);
    println!("Status: {}", response.status);
    println!("Body: {}", String::from_utf8_lossy(&response.body));
    println!("Ruby thread object id: {}", response.ruby_thread_object_id);

    runtime.shutdown()?;
    Ok(())
}
