use std::collections::HashMap;
use std::io;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tokio::task::JoinSet;
use crate::messages::Message;

pub fn parse_line<M: for<'a> Deserialize<'a>>(line: Result<String, io::Error>) -> Option<Message<M>> {
    let Ok(line) = line else {
        eprintln!("error reading init from stdin: {}", line.err().unwrap());
        return None;
    };
    let message = serde_json::from_str::<Message<M>>(&line);
    let Ok(message) = message else {
        eprintln!("error decoding message: {} {line}", message.err().unwrap());
        return None;
    };
    return Some(message);
}

pub fn convert_json_value<T: DeserializeOwned>(value: Value) -> Option<T> {
    match serde_json::from_value(value) {
        Ok(value) => Some(value),
        Err(err) => {
            eprintln!("failed to convert json value into required format {err}");
            None
        }
    }
}

pub fn hash_map_to_json_value<V>(map: HashMap<String, V>) -> Value
    where Value: From<V> 
{
    let map = map
        .into_iter()
        .map(|(k, v)| (k, Value::from(v)))
        .collect(); 
    Value::Object(map)
}

pub async fn join_all(mut join_set: JoinSet<()>) {
    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res {
            eprintln!("error joining task {err}");
        }
    }
}