use rayz_edge::*;
use tokio;
mod file_srever;

async fn handle_ctrl_c_signal(tx: tokio::sync::broadcast::Sender<()>) {
    // Handle the ctrl+c signal
    println!("Press Ctrl+C to stop the server");
    tokio::signal::ctrl_c()
        .await
        .expect("Error setting up Ctrl+C signal handler");

    // Send the stop signal to other tasks
    tx.send(()).unwrap();
}

fn main() {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // let (tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    let (tx, rx) = tokio::sync::broadcast::channel::<()>(1);

    rt.spawn(handle_ctrl_c_signal(tx.clone()));

    file_srever::start_file_web_server(
        "/Users/gxt/Documents/RayzView/dist/".to_string(),
        "0.0.0.0:8080".to_string(),
        rx,
    );
    
    rt.block_on(start_web_server(
        tx.clone().subscribe(),
        " ".to_string(),
        " ".to_string(),
    ));

}
