use axum::routing::Router;
use tower_http::{services::ServeDir, trace::TraceLayer};

use axum::{
    extract::{Json, Path, Query},
    routing::{get, post},
};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::{collections::HashMap, process::Stdio};

use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

// 全局静态变量，存储应用程序子进程状态
static APP_PROCESS: Lazy<Mutex<Option<tokio::process::Child>>> = Lazy::new(|| Mutex::new(None));

fn pick_cli_params(params: HashMap<String, Value>) -> Vec<String> {
    let mut args = Vec::new();
    for (key, val) in params.iter() {
        match key.as_str() {
            "filePath" => match val {
                Value::String(val) => {
                    args.push("-i".to_owned());
                    args.push(val.to_owned())
                }
                _ => {}
            },
            "lidarModel" => {
                args.push("--model".to_owned());
                args.push(val.as_str().unwrap().to_owned());
            }
            "url" => {
                args.push("-i".to_owned());
                args.push(val.as_str().unwrap().to_owned());
            }
            _ => {}
        }
    }
    args
}

async fn handle_connect_post(
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    let mut args: Vec<String> = Vec::new();
    if let Some(json) = json {
        args = pick_cli_params(json.0);
    }
    if let Some(query) = query {
        if query["action"] == "stop" {
            do_stop_lidar_app().await;
        } else if query["action"] == "start" {
            do_start_lidar_app(args).await;
        }
    }

    Json(json!({"status": "ok"}))
}

async fn handle_replay_post(
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    let mut args = Vec::new();
    if let Some(json) = json {
        args = pick_cli_params(json.0);
    }
    if let Some(query) = query {
        if query["action"] == "stop" {
            do_stop_lidar_app().await;
        } else if query["action"] == "start" {
            do_start_lidar_app(args).await;
        }
    }
    // 测试用例
    // if do_check_lidar_app().await == false {
    //     println!("lidar app process not exist, start it!");
    //     do_start_lidar_app(args).await;
    // } else {
    //     println!("lidar app process exist,stop it!");
    //     do_stop_lidar_app().await;
    // }
    Json(json!({"status": "ok"}))
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

async fn do_start_lidar_app(mut args: Vec<String>) -> bool {
    // 确保清理旧的进程
    if do_check_lidar_app().await == true {
        println!("lidar app process exist,stop first!");
        do_stop_lidar_app().await;
    }

    // 确保提供lidar model
    if !args.contains(&"--model".to_owned()) {
        args.push("--model".to_owned());
        args.push("osprey".to_owned());
    }

    // 确保提供ws给前端读取
    if !args.contains(&"-o".to_owned()) || !args.contains(&"ws://0.0.0.0:12369".to_owned()) {
        args.push("-o".to_owned());
        args.push("ws://0.0.0.0:12369".to_owned());
    }

    // 提供debug信息
    if !args.contains(&"--debug".to_owned()) {
        args.push("--debug".to_owned());
    }

    // 回放默认循环播放
    if !args.contains(&"-l".to_owned()) && !args.contains(&"://".to_owned()) {
        args.push("-l".to_owned());
        args.push("-1".to_owned());
    }

    println!("args:{:?}", args);
    let mut app_process = APP_PROCESS.lock().await;
    if app_process.is_none() {
        println!("start bin/rayz_lidar_app with args:{:?}", args);
        let child = tokio::process::Command::new(
            "/Users/xipeng/Documents/Gits/RayzView/src-tauri/crates/RayzEdge/bin/rayz_lidar_app",
        )
        .args(args)
        .stdout(Stdio::inherit())
        .spawn();

        match child {
            Ok(child) => {
                *app_process = Some(child);
                return true;
            }
            Err(err) => {
                println!("start bin/rayz_lidar_app failed! err:{:?}", err);
                return false;
            }
        }
    }
    return false;
}

pub async fn start_web_server(stop_tx: tokio::sync::broadcast::Sender<()>, app_path: String) {
    // let mut stop_rx = stop_tx.subscribe();
    // rt.spawn(async move {
    //     stop_rx.recv().await.unwrap();
    //     do_stop_lidar_app().await;
    // });
    println!("start http server!");

    let app: Router = Router::new()
        .route("/connect", post(handle_connect_post))
        .route("/replay", post(handle_replay_post))
        .layer(CorsLayer::permissive());

    let mut stop_rx = stop_tx.subscribe();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:15001")
        .await
        .unwrap();

    println!("serve http server!");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            stop_rx.recv().await.unwrap();
        })
        .await
        .unwrap();

    do_stop_lidar_app().await; //确保清理子进程
    println!("stop http server!");
}
