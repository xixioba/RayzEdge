use axum::routing::{get_service, Router};
use std::thread;
use tower_http::{services::ServeDir, trace::TraceLayer};

async fn http_serve_static_file(
    route: &str,
    dir: &str,
    addr_port: &str,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let router = Router::new()
        .layer(TraceLayer::new_for_http())
        .nest_service(route, get_service(ServeDir::new(dir).precompressed_br()));

    let listener = tokio::net::TcpListener::bind(addr_port).await.unwrap();
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            stop_rx.await.ok();
        })
        .await
        .unwrap();

    println!("stop http file server!");
}

pub fn start_file_web_server(dir: String, url: String, stop: tokio::sync::oneshot::Receiver<()>) {
    thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(http_serve_static_file("/", &dir, &url, stop));
    });
}
