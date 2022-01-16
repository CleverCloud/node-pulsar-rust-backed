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
use neon::result::Throw;
use std::env;
use std::sync::atomic::AtomicUsize;

use pulsar::{
    message::proto, producer, Authentication, Consumer, DeserializeMessage, Error as PulsarError,
    Executor, Payload, Producer, Pulsar, SerializeMessage, SubType, TokioExecutor,
};
use serde::{Deserialize, Serialize};

// Create our objects first

// This will represent a Pulsar client
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

// This will represent a Pulsar Producer
struct JsPulsarProducer {
    producer: Producer<TokioExecutor>,
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

// We are creating one and only one Tokio runtime, and we will use it everywhere (should work for 99% of usage, and if not, we can invent something another day)
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

// Utils function to get some data from JS calling function

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
    Some(
        args_obj?
            .get(cx, obj_field_name)
            .ok()?
            .downcast::<JsString, _>(cx)
            .ok()?
            .value(cx),
    )
}

// Function that will be exposed to the JS

fn get_pulsar(mut cx: FunctionContext) -> JsResult<JsBox<Arc<JsPulsar>>> {
    // Get the arg obj from js
    let args_obj = cx
        .argument_opt(0)
        .and_then(|a| a.downcast::<JsObject, _>(&mut cx).ok());

    // Find the configuration from js, env or use default
    let addr_from_js =
        get_string_member_or_env(&mut cx, args_obj, "url", "ADDON_PULSAR_BINARY_URL")
            .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());

    let token_from_js = get_string_member_or_env(&mut cx, args_obj, "token", "ADDON_PULSAR_TOKEN");

    // Need to integrate a log system
    //println!("url : {}", addr_from_js);
    //println!("token : {:?}", token_from_js);

    // enter to the tokio thread
    RUNTIME.block_on(async move {
        let mut builder = Pulsar::builder(addr_from_js, TokioExecutor);

        // Authentication ? (we will need to add other auth method here)
        if let Some(token) = token_from_js {
            let authentication = Authentication {
                name: "token".to_string(),
                data: token.into_bytes(),
            };
            builder = builder.with_auth(authentication);
        }

        // return the Pulsar object
        return builder
            .build()
            .await
            .map_or_else(|e| Err(Throw), |pulsar| Ok(cx.boxed(JsPulsar::new(pulsar))));
    })
}

fn get_pulsar_producer(mut cx: FunctionContext) -> JsResult<JsBox<Arc<JsPulsarProducer>>> {
    // get the option on the second optional argument
    let args_obj = cx
        .argument_opt(1)
        .and_then(|a| a.downcast::<JsObject, _>(&mut cx).ok());

    // get the pulsar object
    let pulsar_arc = Arc::clone(&&cx.argument::<JsBox<Arc<JsPulsar>>>(0)?);

    // Topic configuration
    let topic_from_js = get_string_member_or_env(&mut cx, args_obj, "topic", "ADDON_PULSAR_TOPIC")
        .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

    // Enter Tokio
    RUNTIME.block_on(async {
        let mut producer = pulsar_arc
            .pulsar
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

        // return the producer
        Ok(cx.boxed(JsPulsarProducer::new(producer)))
    })
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    // Here we bind the functions accessible from the JS (and used by glue code to make sense of it)

    cx.export_function("getPulsar", get_pulsar)?;
    cx.export_function("getPulsarProducer", get_pulsar_producer)?;

    Ok(())
}
