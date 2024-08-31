use axum::routing::Router;
use tower_http::{services::ServeDir, trace::TraceLayer};

use axum::{
    extract::{Json, Path, Query, State},
    routing::{get, post},
};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::sync::Arc;
use std::{collections::HashMap, process::Stdio};

use regex::Regex;
use tokio::{io::AsyncBufReadExt, sync::Mutex};
use tower_http::cors::CorsLayer;

// log页测试数据用
use chrono::{DateTime, Duration, Local, TimeZone}; // 导入需要的模块
use rand::{self, Rng};

// 全局静态变量，存储应用程序子进程状态
static APP_PROCESS: Lazy<Mutex<Option<tokio::process::Child>>> = Lazy::new(|| Mutex::new(None));
static UTIL_PROCESS: Lazy<Mutex<Option<tokio::process::Child>>> = Lazy::new(|| Mutex::new(None));

#[derive(Debug)]
struct AppBase {
    app_path: String,
    app_process_child: Option<tokio::process::Child>,
    util_path: String,
    util_process_child: Option<tokio::process::Child>,
}

static APP_BASE: Lazy<Mutex<Option<AppBase>>> =
    Lazy::new(|| tokio::task::block_in_place(|| Mutex::new(None)));

// 用来在不同handler之间传递和共享状态
#[derive(Debug, Copy, Clone)]
struct AppState {
    app_path: &'static str,
    util_path: &'static str,
    loaded_state: bool,
    loaded_frame: u32,
    current_frame: i32,
}

fn val_to_string(val: Option<&Value>) -> String {
    match val {
        Some(Value::String(val)) => val.to_owned(),
        Some(Value::Number(val)) => val.to_string(),
        _ => "".to_owned(),
    }
}

fn pick_util_cli_params(
    params: HashMap<String, Value>,
    action: String,
    group: String,
) -> Vec<String> {
    let mut args = Vec::new();
    println!(
        "pick_util_cli_params with params: {:#?} group: {:?}",
        params, group
    );
    match group.as_str() {
        "net" => match action.as_str() {
            "get" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("get".to_owned());
                args.push("all".to_owned());
            }
            "set" => {
                args.push(val_to_string(params.get("IP_old")));
                args.push("debug".to_owned());
                args.push("set".to_owned());
                args.push("net".to_owned());
                args.push(val_to_string(params.get("IP")));
                args.push(val_to_string(params.get("destinationIP")));
                args.push(val_to_string(params.get("destinationPort")));
                args.push(val_to_string(params.get("broudcastIP")));
                args.push(val_to_string(params.get("Gateway")));
                args.push(val_to_string(params.get("Mask")));
                args.push(val_to_string(params.get("HeartPort")));
                args.push(val_to_string(params.get("Mac")));
            }
            _ => {}
        },
        "sn" => match action.as_str() {
            "get" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("debug".to_owned());
                args.push("get".to_owned());
                args.push("sn".to_owned());
            }
            "set" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("debug".to_owned());
                args.push("set".to_owned());
                args.push("sn".to_owned());
            }
            _ => {}
        },
        "reg" => match action.as_str() {
            "get" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("debug".to_owned());
                args.push("get".to_owned());
                args.push("reg".to_owned());
                args.push(val_to_string(params.get("regAddr")));
            }
            "set" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("debug".to_owned());
                args.push("set".to_owned());
                args.push("reg".to_owned());
                args.push(val_to_string(params.get("regAddr")));
                args.push(val_to_string(params.get("regValue")));
            }
            _ => {}
        },
        "fps" => match action.as_str() {
            "get" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("debug".to_owned());
                args.push("get".to_owned());
                args.push("fps".to_owned());
            }
            "set" => {
                args.push(val_to_string(params.get("ipv4")));
                args.push("debug".to_owned());
                args.push("set".to_owned());
                args.push("fps".to_owned());
                args.push(val_to_string(params.get("frameRate")));
            }
            _ => {}
        },
        "reboot" => {
            args.push(val_to_string(params.get("ipv4")));
            args.push("reboot".to_owned());
        }
        _ => {}
    }

    args
}

async fn handle_control_post(
    State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    let mut args = Vec::new();
    if let Some(query) = query {
        let state = state.lock().await;
        // net,reg,reboot
        let group = match query.contains_key("group") {
            true => query["group"].to_string(),
            false => "".to_string(),
        };
        if query["action"] == "get" {
            if let Some(json) = json {
                args = pick_util_cli_params(json.0, "get".to_string(), group);
                // println!("测试args: {:?}", args);
            }
            return do_start_lidar_util(args, state.util_path.to_string()).await;
        } else if query["action"] == "set" {
            if let Some(json) = json {
                args = pick_util_cli_params(json.0, "set".to_string(), group);
            }
            return do_start_lidar_util(args, state.util_path.to_string()).await;
        }
    }
    Json(json!({"status": "ok"}))
}

async fn handle_settings_post(
    // State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    if let Some(query) = query {
        if query["action"] == "get" {
            // println!("test...control_get!!!");
            let ip = "192.168.0.2".to_string();
            let dip = "192.168.0.3".to_string();
            let dport = "2368".to_string();
            let bip = "192.168.0.255".to_string();
            let mask = "255.255.255.0".to_string();
            let gateway = "192.168.0.1".to_string();
            let mac = "00:0A:35:00:EB:2A".to_string();
            let hport = "56789".to_string();
            return Json(json!({
                "IP": ip,
                "DIP": dip,
                "DPort": dport,
                "BIP": bip,
                "Mask": mask,
                "Gateway": gateway,
                "MAC": mac,
                "HPort": hport
            }));
        } else if query["action"] == "set" {
            println!("{:?}", json)
        } else if query["action"] == "address_get" {
            println!("address_get...test!!");
            let address_value = "123007".to_string();
            return Json(json!({
                "address_value": address_value
            }));
        } else if query["action"] == "address_set" {
            println!("{:?}", json)
        }
    }
    Json(json!({"status": "ok"}))
}

// log页测试数据用
fn random_date(start_date: DateTime<Local>, end_date: DateTime<Local>) -> DateTime<Local> {
    let delta = end_date.signed_duration_since(start_date);
    let days: f64 = delta.num_days() as f64 * rand::random::<f64>();
    start_date + Duration::days(days as i64)
}

fn generate_mock_data(count: usize) -> Vec<MockData> {
    let mut data = Vec::new();
    let types = ["error", "warn", "info"];
    let start_date = Local.ymd(2020, 1, 1).and_hms(0, 0, 0);
    let end_date = Local::now();
    for i in 0..count {
        let type_index = rand::thread_rng().gen_range(0..types.len());
        let info_levels = types[type_index];
        data.push(MockData {
            index: i + 1,
            date: random_date(start_date, end_date),
            info_levels: info_levels.to_string(),
            content: format!("No. {}, Grove St, Los Angeles", i + 100),
        });
    }
    data
}

use serde::{Serialize, Serializer};

#[derive(Serialize)]
struct MockData {
    index: usize,
    #[serde(serialize_with = "serialize_datetime")]
    date: DateTime<Local>,
    info_levels: String,
    content: String,
}

// 手动实现 DateTime<Local> 的序列化方法
fn serialize_datetime<S>(datetime: &DateTime<Local>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // 使用 chrono 提供的 to_rfc3339 方法将日期时间转换为 RFC3339 格式的字符串
    let formatted = datetime.to_rfc3339();
    // 调用 serde 的序列化方法将字符串序列化为 JSON
    serializer.serialize_str(&formatted)
}

async fn handle_log_get(
    // State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    // json: Option<Json<Vec<HashMap<String, Value>>>>,
) -> Json<Value> {
    // let args: Vec<String> = Vec::new();
    // if let Some(json) = json {
    //     args = pick_app_cli_params(json.0);
    // }
    if let Some(query) = query {
        if query["action"] == "log_get" {
            // let table_data = generate_mock_data(101);
            let mock_data = generate_mock_data(101);
            let table_data = serde_json::to_string(&mock_data).unwrap();

            // let json_data = serde_json::to_value(table_data).unwrap();
            return Json(json!(table_data));
        } else {
            println!("status_get_null!!!");
        }
    }

    println!("handle_status_get!!!");
    Json(json!({"status": "ok"}))
}

async fn handle_register_post(
    // State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    if let Some(query) = query {
        if query["action"] == "get" {
            // println!("test...control_get!!!");
            let input1 = "0x00000000".to_string();
            let input2 = "0x00000000".to_string();
            let input3 = "0x00000000".to_string();
            let input4 = "0x00000000".to_string();
            let input5 = "0x00000000".to_string();
            let input6 = "0x00000000".to_string();
            let input7 = "0x00000000".to_string();
            let input8 = "0x00000000".to_string();
            let input9 = "0x00000000".to_string();
            let input10 = "0x00000000".to_string();
            let input11 = "0x00000000".to_string();
            let input12 = "0x00000000".to_string();
            let input13 = "0x00000000".to_string();
            let input14 = "0x00000000".to_string();
            let input15 = "0x00000000".to_string();
            let input16 = "0x00000000".to_string();
            let input17 = "0x00000000".to_string();
            let input18 = "0x00000000".to_string();
            let input19 = "0x00000000".to_string();
            let input20 = "0x00000000".to_string();
            let input21 = "0x00000000".to_string();
            let input22 = "0x00000000".to_string();
            let input23 = "0x00000000".to_string();
            let input24 = "0x00000000".to_string();
            let input25 = "0x00000000".to_string();
            let input26 = "0x00000000".to_string();
            let input27 = "0x00000000".to_string();
            let input28 = "0x00000000".to_string();
            let input29 = "0x00000000".to_string();
            let input30 = "0x00000000".to_string();
            let input31 = "0x00000000".to_string();
            let input32 = "0x00000000".to_string();
            let input33 = "0x00000000".to_string();
            let input34 = "0x00000000".to_string();
            let input35 = "0x00000000".to_string();
            let input36 = "0x00000000".to_string();

            return Json(json!({
                "input1": input1,
                "input2": input2,
                "input3": input3,
                "input4": input4,
                "input5": input5,
                "input6": input6,
                "input7": input7,
                "input8": input8,
                "input9": input9,
                "input10": input10,
                "input11": input11,
                "input12": input12,
                "input13": input13,
                "input14": input14,
                "input15": input15,
                "input16": input16,
                "input17": input17,
                "input18": input18,
                "input19": input19,
                "input20": input20,
                "input21": input21,
                "input22": input22,
                "input23": input23,
                "input24": input24,
                "input25": input25,
                "input26": input26,
                "input27": input27,
                "input28": input28,
                "input29": input29,
                "input30": input30,
                "input31": input31,
                "input32": input32,
                "input33": input33,
                "input34": input34,
                "input35": input35,
                "input36": input36
            }));
        } else if query["action"] == "set" {
            println!("{:?}", json)
        }
    }
    Json(json!({"status": "ok"}))
}

async fn handle_view_post(
    // State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    // json: Option<Json<HashMap<String, Value>>>,
) -> Json<Value> {
    if let Some(query) = query {
        if query["action"] == "lidarInfo_get" {
            // println!("test...control_get!!!");
            let spin_rate = "200rmp".to_string();
            let ptp = "FreeRun/Tracking/Locked/Frozen".to_string();
            let gps = "Locked/Unlock".to_string();
            let model = "AT128E2X".to_string();
            let sn = "AT3ECE52923ECE52".to_string();
            let mac_address = "EC:9F:1E:CD:1F".to_string();
            let software_version = "1.0.1.2".to_string();
            let controller_firmware_version = "1.0.1.2".to_string();
            let startup_count = "5".to_string();
            let internal_temperature = "36.63&deg;C".to_string();
            let total_operation_time = "1h32min".to_string();

            return Json(json!({
                "spin_rate": spin_rate,
                "ptp": ptp,
                "gps": gps,
                "model": model,
                "sn": sn,
                "mac_address": mac_address,
                "software_version": software_version,
                "controller_firmware_version": controller_firmware_version,
                "startup_count": startup_count,
                "internal_temperature": internal_temperature,
                "total_operation_time": total_operation_time
            }));
        } else if query["action"] == "set" {
        }
    }
    Json(json!({"status": "ok"}))
}

fn pick_app_cli_params(params: Vec<HashMap<String, Value>>) -> Vec<String> {
    let mut args = Vec::new();
    for group in params {
        for (key, val) in group.iter() {
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
                    if let Some(val) = val.as_str() {
                        match val {
                            "H260" => args.push("h2a+".to_owned()),
                            "H260R" => args.push("h2a".to_owned()),
                            "W100" => args.push("m2w".to_owned()),
                            "W100P" => args.push("m2w+".to_owned()),
                            "V80" => args.push("m2v".to_owned()),
                            _ => args.push(val.to_owned()),
                        }
                    }
                }
                "url" => {
                    args.push("-i".to_owned());
                    if let Some(val) = val.as_str() {
                        args.push(val.to_owned());
                    }
                }
                "recordPath" => {
                    args.push("--record".to_owned());
                    if let Some(val) = val.as_str() {
                        args.push(val.to_owned());
                    }
                    // args.push("--raw".to_owned());
                }
                "uid" => {
                    args.push("--id".to_owned());
                    match val {
                        Value::String(val) => args.push(val.to_owned()),
                        Value::Number(val) => args.push(val.to_string()),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
    args
}

async fn handle_connect_post(
    State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<Vec<HashMap<String, Value>>>>,
) -> Json<Value> {
    let mut args: Vec<String> = Vec::new();
    if let Some(json) = json {
        args = pick_app_cli_params(json.0);
    }
    if let Some(query) = query {
        let state = state.lock().await;
        if query["action"] == "stop" {
            do_stop_lidar_app().await;
        } else if query["action"] == "start" {
            do_start_lidar_app(args, state.app_path.to_string()).await;
        }
    }

    Json(json!({"status": "ok"}))
}

async fn handle_merge_post(
    State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<Vec<HashMap<String, Value>>>>,
) -> Json<Value> {
    let mut args: Vec<String> = Vec::new();
    if let Some(json) = json {
        args = pick_app_cli_params(json.0);
    }
    if let Some(query) = query {
        if query["action"] == "stop" {
            do_stop_lidar_app().await;
        } else if query["action"] == "start" {
            let state = state.lock().await;
            do_start_lidar_app(args, state.app_path.to_string()).await;
        }
    }

    Json(json!({"status": "ok"}))
}

async fn handle_replay_post(
    State(state): State<Arc<Mutex<AppState>>>,
    query: Option<Query<HashMap<String, String>>>,
    json: Option<Json<Vec<HashMap<String, Value>>>>,
) -> Json<Value> {
    let mut rate = 0;
    let mut state = state.lock().await;
    let mut args = Vec::new();
    if let Some(json) = json {
        if let Some(first_item) = json.0.first() {
            if let Some(val) = first_item.get("play_rate") {
                match val.as_u64() {
                    Some(val) => {
                        rate = val;
                    }
                    None => {
                        rate = 0;
                    }
                }
                println!("更新play_rate: {:?}", rate);
            }
        }
        // println!("test...参数{:?}",json.0.get("currentTime"));
        args = pick_app_cli_params(json.0);
    }
    if let Some(query) = query {
        if query["action"] == "stop" {
            println!("test...stop请求");
            do_stop_lidar_app().await;
            return Json(json!({"status": "success"}));
        } else if query["action"] == "start" {
            println!("test...start请求");
            do_start_lidar_app(args, state.app_path.to_string()).await;
            return Json(json!({"status": "success"}));
        } else if query["action"] == "loaded_state" {
            println!("test...state请求");
            state.loaded_frame = state.loaded_frame + 10000;
            if state.loaded_frame == 60000 {
                state.loaded_state = true;
            } else {
                state.loaded_state = false;
            }
            return Json(json!({
                "loaded_state": state.loaded_state,
                "loaded_frame": state.loaded_frame
            }));
        } else if query["action"] == "play_state" {
            println!("test...state请求");
            if state.current_frame == 60 {
                state.current_frame = 60;
            } else {
                state.current_frame = state.current_frame + 1;
            }
            println!("测试当前frame {}", state.current_frame);
            return Json(json!({
                "current_frame":state.current_frame
            }));
        } else if query["action"] == "backword" {
            println!("test...backword请求");
            let target_frame = state.current_frame - rate as i32;
            if target_frame <= 0 {
                state.current_frame = 0;
                return Json(json!({
                    "target_frame": 0
                }));
            } else if target_frame > 0 {
                state.current_frame = target_frame;
            }
            println!("后退目标帧target_frame: {}", target_frame);
            return Json(json!({
                "target_frame": target_frame
            }));
        } else if query["action"] == "forword" {
            println!("test...forword请求");
            let target_frame = state.current_frame + rate as i32;
            if target_frame >= 60 {
                state.current_frame = 60;
                return Json(json!({
                    "target_frame": 60
                }));
            } else if target_frame < 60 {
                state.current_frame = target_frame;
            }
            println!("前进目标帧target_frame: {}", target_frame);
            return Json(json!({
                "target_frame": target_frame,
            }));
        } else if query["action"] == "skip" {
            println!("test...skip请求");
        } else if query["action"] == "rate" {
            println!("test...rate请求");
        } else if query["action"] == "loopplay" {
            println!("test...loopplay请求");
        }
    }
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
    // if !args.contains(&"--debug".to_owned()) {
    //     args.push("--debug".to_owned());
    // }

    // 回放默认循环播放
    if !args.contains(&"-l".to_owned()) && !args.contains(&"://".to_owned()) {
        args.push("-l".to_owned());
        args.push("-1".to_owned());
    }

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

async fn do_start_lidar_util(args: Vec<String>, util_path: String) -> Json<Value> {
    // check if args contain "" invalid param
    if args.len() == 0 || args.contains(&"".to_string()) {
        println!("invalid param! {:?}", args);
        return Json(json!({"status": "invalid param","Ret":"Failed"}));
    }
    let mut json_obj = json!({});
    let util_process = UTIL_PROCESS.lock().await;
    if util_process.is_none() {
        println!("start {:?} with args:{:?}", util_path, args);
        let child;
        #[cfg(target_os = "windows")]
        {
            child = tokio::process::Command::new(util_path)
                .args(args)
                .creation_flags(0x08000000) // hide terminal for windows
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn();
        }
        #[cfg(not(target_os = "windows"))]
        {
            child = tokio::process::Command::new(util_path)
                .args(args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn();
        }

        if let Ok(mut child) = child {
            if let Some(stdout) = child.stderr.take() {
                let mut stderr_reader = tokio::io::BufReader::new(stdout).lines();
                while let Some(line) = stderr_reader.next_line().await.unwrap() {
                    if let Ok(re) = Regex::new(
                        r#"^\[(?P<time>.*) (?P<level>.*)\] - (?P<key>.*): \"?(?P<val>.*?)\"?$"#,
                    ) {
                        if let Some(caps) = re.captures(&line) {
                            println!(
                                "util filter: {} {} {} {}",
                                &caps["time"], &caps["level"], &caps["key"], &caps["val"]
                            );
                            json_obj[&caps["key"]] = json!(caps["val"]);
                        } else {
                            println!("util output: {}", line);
                        }
                    }
                }
                println!("return {:#?}", json_obj);
            }
            // *util_process = Some(child);
            child.kill().await.expect("kill failed");
            match child.wait().await {
                Ok(status) => println!("child process exited with status {:?}", status),
                Err(e) => println!("error while waiting for child process: {}", e),
            }
        }
    }
    return Json(json_obj);
}

pub async fn start_web_server(
    mut stop_rx: tokio::sync::broadcast::Receiver<()>,
    app_path: String,
    util_path: String,
    tcp_port: u16,
) {
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
    let shared_state = Arc::new(Mutex::new(AppState {
        app_path: Box::leak(app_path.into_boxed_str()),
        util_path: Box::leak(util_path.into_boxed_str()),
        loaded_state: false,
        loaded_frame: 0,
        current_frame: 0,
    }));

    let app: Router = Router::new()
        .route("/connect", post(handle_connect_post))
        .route("/record", post(handle_connect_post))
        .route("/replay", post(handle_replay_post))
        .route("/control", post(handle_control_post))
        .route("/settings", post(handle_settings_post))
        .route("/log", get(handle_log_get))
        .route("/register", post(handle_register_post))
        .route("/view", post(handle_view_post))
        .route("/merge", post(handle_merge_post))
        .with_state(shared_state)
        .layer(CorsLayer::permissive());

    let addr = format!("0.0.0.0:{}", tcp_port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            stop_rx.recv().await.unwrap();
        })
        .await
        .unwrap();

    do_stop_lidar_app().await; //确保清理子进程
    println!("stop http server!");
}
