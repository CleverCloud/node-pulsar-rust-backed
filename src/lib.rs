use neon::prelude::*;
use std::borrow::Borrow;

use neon::prelude::*;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

#[macro_use]
extern crate serde;

use neon_runtime;
use neon_runtime::raw;

use std::sync::Arc;

use neon::macro_internal::Env;
use std::env;
use std::sync::atomic::AtomicUsize;
use neon::result::Throw;

use pulsar::{
    message::proto, producer, Authentication, Consumer, DeserializeMessage, Error as PulsarError,
    Executor, Payload, Producer, Pulsar, SerializeMessage, SubType, TokioExecutor,
};
use serde::{Deserialize, Serialize};

struct JsPulsar {
    pulsar: Pulsar<TokioExecutor>,
}

impl JsPulsar {
    fn new(pulsar: Pulsar<TokioExecutor>) -> Arc<Self> {
        Arc::new(Self { pulsar })
    }
}

impl Finalize for JsPulsar {
    fn finalize<'a, C: Context<'a>>(self, cx: &mut C) {
        // Maybe I need to cleanup stuff here, but i'm not sure
    }
}

struct JsPulsarProducer {
    producer: Producer<TokioExecutor>
}

impl JsPulsarProducer {
    fn new(producer: Producer<TokioExecutor>) -> Arc<Self> {
        Arc::new(Self { producer })
    }
}

impl Finalize for JsPulsarProducer {
    fn finalize<'a, C: Context<'a>>(self, cx: &mut C) {
        // Maybe I need to cleanup stuff here, but i'm not sure
    }
}

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());




/*
impl Managed for JsPulsar {
    fn to_raw(self) -> raw::Local {
        self.0
    }

    fn from_raw(_env: Env, h: raw::Local) -> Self {
        JsPulsar(h)
    }
}
*/

/*
impl Value for JsPulsar {
    fn to_string<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsString> {
        return self.to_string(C)
    }
    fn as_value<'a, C: Context<'a>>(self, _: &mut C) -> Handle<'a, JsValue> {
        self.as_value(C)
    }
}
*/
fn get_string_member_or_env(
    cx: &mut FunctionContext,
    args_obj: Option<Handle<JsObject>>,
    obj_field_name: &str,
    env_var_name: &str,
) -> Option<String> {
    get_string_member(cx, args_obj, obj_field_name).or_else(|| env::var(env_var_name).ok())
}

fn get_string_member(
    cx: &mut FunctionContext,
    args_obj: Option<Handle<JsObject>>,
    obj_field_name: &str,
) -> Option<String> {
    Some(args_obj?
        .get(cx, obj_field_name).ok()?
        .downcast::<JsString, _>(cx).ok()?
        .value(cx))
}

fn get_pulsar(mut cx: FunctionContext) -> JsResult<JsBox<Arc<JsPulsar>>> {
    // Get the arg obj from js
    let args_obj = cx.argument::<JsObject>(0).ok();

    // Find the configuration from js, env or use default
    let addr_from_js = get_string_member_or_env(&mut cx, args_obj, "url", "PULSAR_BINARY_URL")
        .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());
   // let topic_from_js = get_string_member_or_env(&mut cx, args_obj, "topic", "PULSAR_TOPIC")
    //    .unwrap_or_else(|| "non-persistent://public/default/test".to_string());
    let token_from_js = get_string_member_or_env(&mut cx, args_obj, "token", "PULSAR_TOKEN");

    println!("url : {}", addr_from_js);
    //println!("topic : {}", topic_from_js);
    //println!("token : {:?}", token_from_js);

    RUNTIME.block_on(async move {
        let mut builder = Pulsar::builder(addr_from_js, TokioExecutor);
        if let Some(token) = token_from_js {
            let authentication = Authentication {
                name: "token".to_string(),
                data: token.into_bytes(),
            };
            builder = builder.with_auth(authentication);
        }

        return builder.build().await.map_or_else(|e| Err(Throw), |pulsar| Ok(cx.boxed(JsPulsar::new(pulsar))));

    })
}

fn get_pulsar_producer(mut cx: FunctionContext) -> JsResult<JsBox<Arc<JsPulsarProducer>>> {
        let args_obj = cx.argument::<JsObject>(1).ok();

        let pulsar_arc = Arc::clone(&&cx.argument::<JsBox<Arc<JsPulsar>>>(0)?);

        let topic_from_js = get_string_member_or_env(&mut cx, args_obj, "topic", "PULSAR_TOPIC")
            .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

    RUNTIME.block_on(async {

        let mut producer = pulsar_arc.pulsar
            .producer()
            .with_topic(topic_from_js)
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        Ok(cx.boxed(JsPulsarProducer::new(producer)))

    })
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("getPulsar", get_pulsar)?;
    cx.export_function("getPulsarProducer", get_pulsar_producer)?;

    Ok(())
}
