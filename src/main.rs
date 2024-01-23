use axum::{
    extract::{Json, Path, Query},
    routing::{get, post},
    Router,
};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::{collections::HashMap, process::Stdio};

use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

// 全局静态变量，存储应用程序子进程状态
static APP_PROCESS: Lazy<Mutex<Option<tokio::process::Child>>> = Lazy::new(|| Mutex::new(None));

async fn handle_connect_post(
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    if let Some(query) = query {
        for (key, value) in query.iter() {
            println!("key:{},value:{}", key, value);
        }
    }

    if let Some(map) = json {
        for (key, val) in map.iter() {
            println!("{}: {:?}", key, val);
        }
    }

    Json(json!({"status": "ok"}))
}

async fn handle_replay_post(
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    let mut params = HashMap::<String, Value>::new();
    if let Some(map) = json {
        params = map.0;
        // for (key, val) in map.iter() {
        //     println!("{}: {:?}", key, val);
        // }
    }
    // 测试用例
    if do_check_lidar_app().await == false {
        println!("lidar app process not exist, start it!");
        do_start_lidar_app(params).await;
    } else {
        println!("lidar app process exist,stop it!");
        do_stop_lidar_app().await;
    }
    // if let Some(query) = query {
    //     println!("query {:?}", query);
    //     // for (key, value) in query.iter() {
    //     //     println!("key:{},value:{}", key, value);
    //     // }
    //     if query["action"] == "stop" {
    //         do_stop_lidar_app().await;
    //     } else if query["action"] == "start" {
    //         do_start_lidar_app().await;
    //     }
    // }

    Json(json!({"status": "ok"}))
}

async fn handle_ctrl_c_signal(tx: tokio::sync::broadcast::Sender<()>) {
    // Handle the ctrl+c signal
    println!("Press Ctrl+C to stop the server");
    tokio::signal::ctrl_c()
        .await
        .expect("Error setting up Ctrl+C signal handler");

    // Send the stop signal to other tasks
    tx.send(()).unwrap();
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, mut rx) = tokio::sync::broadcast::channel::<()>(1);

    tokio::spawn(handle_ctrl_c_signal(tx.clone()));
    tokio::spawn(async move {
        tx.subscribe().recv().await.unwrap();
        do_stop_lidar_app().await;
    });

    // start_file_web_server("./".to_string(), "0.0.0.0:8080".to_string(), tx.subscribe());

    let app = Router::new()
        .route("/connect", post(handle_connect_post))
        .route("/replay", post(handle_replay_post))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:15001")
        .await
        .unwrap();

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            rx.recv().await.unwrap();
        })
        .await
        .unwrap();

    do_stop_lidar_app().await; //确保清理子进程
    println!("stop http server!");
}

async fn do_check_lidar_app() -> bool {
    let app_process = APP_PROCESS.lock().await;
    if app_process.is_none() {
        println!("lidar app process check no detect!");
        return false;
    } else {
        println!("lidar app process check detect!");
        return true;
    }
}

async fn do_stop_lidar_app() {
    let mut app_process = APP_PROCESS.lock().await;
    if let Some(mut child) = app_process.take() {
        child.kill().await.expect("kill failed");
        match child.wait().await {
            Ok(status) => println!("child process exited with status {:?}", status),
            Err(e) => println!("error while waiting for child process: {}", e),
        }
        println!("child process has finished!")
    } else {
        println!("no child process need to kill!")
    }
}

async fn do_start_lidar_app(params: HashMap<String, Value>) -> bool {
    let mut args = Vec::new();
    for (key, val) in params.iter() {
        match key.as_str() {
            "file_path" => match val {
                Value::String(val) => {
                    args.push("-i".to_owned());
                    args.push(val.to_owned())
                }
                _ => {}
            },
            "lidar_model" => {
                args.push("--model".to_owned());
                args.push(val.as_str().unwrap().to_owned());
            }
            _ => {}
        }
    }
    // 确保提供ws给前端读取
    args.push("-o".to_owned());
    args.push("ws://0.0.0.0:2369".to_owned());
    // 确保提供debug信息
    args.push("--debug".to_owned());
    // 默认循环播放
    args.push("-l".to_owned());
    args.push("-1".to_owned());
    println!("args:{:?}", args);
    let mut app_process = APP_PROCESS.lock().await;
    if app_process.is_none() {
        // 建立新进程
        // let args = vec![
        //     "-i",
        //     "/Users/xipeng/Documents/Rayz/Data/230928_duge/0928_dg_ip02.pcap",
        //     "--model",
        //     "osprey",
        //     "-o",
        //     "ws://0.0.0.0:2369",
        //     "--debug",
        // ];
        println!("start bin/rayz_lidar_app with args:{:?}", args);
        let child = tokio::process::Command::new("bin/rayz_lidar_app")
            .args(args)
            .stdout(Stdio::inherit())
            .spawn();

        match child {
            Ok(child) => {
                *app_process = Some(child);
                return true;
            }
            Err(_) => {
                return false;
            }
        }
    }
    return false;
}
