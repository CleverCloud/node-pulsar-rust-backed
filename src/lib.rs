use neon::prelude::*;
use std::cell::{Cell, RefCell};
use std::convert::TryInto;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use neon::prelude::*;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

#[macro_use]
extern crate serde;
use neon::result::Throw;
use std::env;
use std::fmt::Formatter;

use futures::{FutureExt, TryStreamExt};
use pulsar::{
    message::proto, producer, Authentication, Consumer, DeserializeMessage, Error as PulsarError,
    Executor, Payload, Pulsar, SerializeMessage, SubType, TokioExecutor,
};
use serde::{Deserialize, Serialize};

struct JsPulsar(Pulsar<TokioExecutor>);

impl Finalize for JsPulsar {
    fn finalize<'a, C: Context<'a>>(self, cx: &mut C) {
        // Maybe I need to cleanup stuff here, but i'm not sure
    }
}

fn getStringMemberOrEnv(
    cx: &mut FunctionContext,
    args_obj: Option<Handle<JsObject>>,
    obj_field_name: &str,
    env_var_name: &str,
) -> Option<String> {
    return args_obj
        .map(|o| {
            o.get(cx, obj_field_name)
                .map(|url| url.downcast::<JsString, _>(cx).unwrap().value(cx))
                .ok()
        })
        .flatten()
        .or_else(|| env::var(env_var_name).ok());
}

fn get_pulsar(mut cx: FunctionContext) -> JsResult<JsBox<JsPulsar>> {
    // Get the arg obj from js
    let args_obj = cx.argument::<JsObject>(0).ok();

    // Find the configuration from js, env or use default
    let addr_from_js = getStringMemberOrEnv(&mut cx, args_obj, "url", "PULSAR_BINARY_URL")
        .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());
    let topic_from_js = getStringMemberOrEnv(&mut cx, args_obj, "topic", "PULSAR_TOPIC")
        .unwrap_or_else(|| "non-persistent://public/default/test".to_string());
    let token_from_js = getStringMemberOrEnv(&mut cx, args_obj, "token", "PULSAR_TOKEN");
    println!("url : {}", addr_from_js);
    println!("topic : {}", topic_from_js);
    println!("token : {:?}", token_from_js);

    RUNTIME.block_on(async {
        let mut builder = Pulsar::builder(addr_from_js, TokioExecutor);
        if let Some(token) = token_from_js {
            let authentication = Authentication {
                name: "token".to_string(),
                data: token.into_bytes(),
            };
            builder = builder.with_auth(authentication);
        }

        let pulsar: Pulsar<_> = builder.build().await.unwrap();

        Ok(cx.boxed(JsPulsar(pulsar)))
    })
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("getPulsar", get_pulsar)?;

    Ok(())
}
