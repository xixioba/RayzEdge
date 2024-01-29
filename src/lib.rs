use axum::routing::Router;
use tower_http::{services::ServeDir, trace::TraceLayer};

use axum::{
    extract::{Json, Path, Query, State},
    routing::{get, post},
};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::{collections::HashMap, process::Stdio};

use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

// 全局静态变量，存储应用程序子进程状态
static APP_PROCESS: Lazy<Mutex<Option<tokio::process::Child>>> = Lazy::new(|| Mutex::new(None));

#[derive(Debug)]
struct AppBase {
    app_path: String,
    app_process_child: Option<tokio::process::Child>,
    util_path: String,
    util_process_child: Option<tokio::process::Child>,
}

static APP_BASE: Lazy<Mutex<Option<AppBase>>> =
    Lazy::new(|| tokio::task::block_in_place(|| Mutex::new(None)));

#[derive(Debug, Copy, Clone)]
struct AppState {
    app_path: &'static str,
}

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
    State(state): State<AppState>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    println!("handle_connect_post {:?}", state.app_path);
    let mut args: Vec<String> = Vec::new();
    if let Some(json) = json {
        args = pick_cli_params(json.0);
    }
    if let Some(query) = query {
        if query["action"] == "stop" {
            do_stop_lidar_app().await;
        } else if query["action"] == "start" {
            do_start_lidar_app(args, state.app_path.to_string()).await;
        }
    }

    Json(json!({"status": "ok"}))
}

async fn handle_replay_post(
    State(state): State<AppState>,
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
            do_start_lidar_app(args, state.app_path.to_string()).await;
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
    println!("do_check_lidar_app!");

    // let base = APP_BASE.lock().await;
    // if let Some(base) = base.as_ref() {
    //     if base.app_process_child.is_none() {
    //         println!("no child process need to kill!");
    //         return true;
    //     } else {
    //         println!("lidar app process check detect!");
    //         return true;
    //     }
    // }
    // println!("lidar app process check err!");
    // return false;
    let app_process = APP_PROCESS.lock().await;
    if app_process.is_none() {
        println!("lidar app process check no detect!");
        return false;
    } else {
        println!("lidar app process check detect!");
        return true;
    }
}

async fn do_stop_lidar_app() -> bool {
    // if let Some(base) = APP_BASE.lock().await.as_mut() {
    //     if base.app_process_child.is_none() {
    //         println!("no child process need to kill!");
    //         return true;
    //     } else if let Some(mut child) = base.app_process_child.take() {
    //         child.kill().await.expect("kill failed");
    //         match child.wait().await {
    //             Ok(status) => {
    //                 println!("child process exited with status {:?}", status);
    //                 return true;
    //             }
    //             Err(e) => {
    //                 println!("error while waiting for child process: {}", e);
    //                 return false;
    //             }
    //         }
    //     } else {
    //         println!("should not reach here!");
    //         return true;
    //     }
    // }

    let mut app_process = APP_PROCESS.lock().await;
    if let Some(mut child) = app_process.take() {
        child.kill().await.expect("kill failed");
        match child.wait().await {
            Ok(status) => println!("child process exited with status {:?}", status),
            Err(e) => println!("error while waiting for child process: {}", e),
        }
        println!("child process has finished!");
    } else {
        println!("no child process need to kill!");
    }
    return true;
}

async fn do_start_lidar_app(mut args: Vec<String>, app_path: String) -> bool {
    println!("do_start_lidar_app 1!");
    // 确保清理旧的进程
    if do_check_lidar_app().await == true {
        println!("lidar app process exist,stop first!");
        do_stop_lidar_app().await;
    }
    println!("do_start_lidar_app! 2");
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
    // let mut app_base = APP_BASE.lock().await;
    // if let Some(base) = APP_BASE.lock().await.as_mut() {
    //     if base.app_process_child.is_none() {
    //         println!("start bin/rayz_lidar_app with args:{:?}", args);
    //         let child = tokio::process::Command::new(&base.app_path)
    //             .args(args)
    //             .stdout(Stdio::inherit())
    //             .spawn();

    //         match child {
    //             Ok(child) => {
    //                 base.app_process_child = Some(child);
    //                 return true;
    //             }
    //             Err(err) => {
    //                 println!("start bin/rayz_lidar_app failed! err:{:?}", err);
    //                 return false;
    //             }
    //         }
    //     }
    // }

    let mut app_process = APP_PROCESS.lock().await;
    if app_process.is_none() {
        println!("start {:?} with args:{:?}", app_path, args);
        let child;
        #[cfg(target_os = "windows")]
        {
            child = tokio::process::Command::new(app_path)
                .args(args)
                .creation_flags(0x08000000) // hide terminal for windows
                .stdout(Stdio::inherit())
                .spawn();
        }
        #[cfg(not(target_os = "windows"))]
        {
            child = tokio::process::Command::new(app_path)
                .args(args)
                .stdout(Stdio::inherit())
                .spawn();
        }

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

pub async fn start_web_server(mut stop_rx: tokio::sync::broadcast::Receiver<()>, app_path: String) {
    println!("start http server!");

    // let mut app_base = APP_BASE.lock().await;
    // if app_base.is_none() {
    //     let base = AppBase {
    //         app_path: app_path.clone(),
    //         app_process_child: None,
    //         util_path: "".to_owned(),
    //         util_process_child: None,
    //     };
    //     *app_base = Some(base);
    // }

    // do_check_lidar_app().await;

    let app: Router = Router::new()
        .route("/connect", post(handle_connect_post))
        .route("/replay", post(handle_replay_post))
        .with_state(AppState {
            app_path: Box::leak(app_path.into_boxed_str()),
        })
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:15001")
        .await
        .unwrap();

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            stop_rx.recv().await.unwrap();
        })
        .await
        .unwrap();

    do_stop_lidar_app().await; //确保清理子进程
    println!("stop http server!");
}
