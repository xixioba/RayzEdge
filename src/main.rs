use axum::{
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};

use rayz_edge::*;
use std::{
    process::{Command, Stdio},
    thread, time,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    start_file_web_server("./".to_string(), "0.0.0.0:8080".to_string(), rx);

    let app = Router::new()
        .route("/", get(root))
        .route("/sdk/start", post(start_sdk))
        .route("/sdk/stop", get(stop_sdk))
        .route("/sdk/pause", get({}))
        .route("/info", get({}))
        .route("/net/set", get({}))
        .route("/net/get", get({}));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:2370").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "Hello, World!"
}

async fn start_sdk(payload: String) {
    println!("payload {payload}");
    if let Ok(parsed_data) = serde_json::from_str::<Value>(&payload) {
        println!("Receive Json data: {:?}", parsed_data);
    }

    let mut child = Command::new("bin/rayz_lidar_app")
        .arg("--help")
        .stdout(Stdio::inherit())
        .spawn()
        .expect("failed to start rayz lidar app");

    thread::sleep(time::Duration::from_secs(1));

    let should_stop = true;
    if should_stop {
        if let Err(_) = child.kill() {
            println!("failed to stop child!");
        } else {
            println!("stop child!");
        }
    }

    if let Err(_) = child.wait() {
        println!("failed to wait child process!");
    } else {
        println!("child process has finished!");
    }
}

async fn stop_sdk() -> &'static str {
    println!("stop sdk");
    "stop sdk"
}
