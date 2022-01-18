use neon::prelude::*;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;

use neon::prelude::*;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

use neon_runtime;
use neon_runtime::raw;

use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use neon::macro_internal::Env;
use neon::result::Throw;
use std::env;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;

use pulsar::{
    message::proto, producer, Authentication, Consumer, DeserializeMessage, Error as PulsarError,
    Executor, Payload, Producer, Pulsar, SerializeMessage, SubType, TokioExecutor,
};
use serde::{Deserialize, Serialize};

use std::thread;

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
    fn new(producer: Producer<TokioExecutor>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { producer }))
    }
}

impl Finalize for JsPulsarProducer {
    fn finalize<'a, C: Context<'a>>(self, cx: &mut C) {
        // Maybe I need to cleanup stuff here, but i'm not sure
    }
}

struct JsPulsarConsumer {
    consumer: Consumer<String, TokioExecutor>,
}

impl JsPulsarConsumer {
    fn new(consumer: Consumer<String, TokioExecutor>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { consumer }))
    }
}

impl Finalize for JsPulsarConsumer {
    fn finalize<'a, C: Context<'a>>(self, cx: &mut C) {
        // Maybe I need to cleanup stuff here, but i'm not sure
    }
}

struct UnstructuredJsMessage<'a> {
    js_object: Handle<'a, JsValue>, //,
                                    //  context: Context<'a>
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

    debug!("pulsar url : {}", addr_from_js);
    debug!("pulsar token : {:?}", token_from_js);

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

fn get_pulsar_producer(mut cx: FunctionContext) -> JsResult<JsBox<Arc<Mutex<JsPulsarProducer>>>> {
    // get the option on the second optional argument
    let args_obj = cx
        .argument_opt(1)
        .and_then(|a| a.downcast::<JsObject, _>(&mut cx).ok());

    // get the pulsar object
    let pulsar_arc = Arc::clone(&&cx.argument::<JsBox<Arc<JsPulsar>>>(0)?);

    // Topic configuration
    let topic_from_js = get_string_member_or_env(&mut cx, args_obj, "topic", "ADDON_PULSAR_TOPIC")
        .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

    debug!("Topic info for new producer : {}", topic_from_js);

    // Enter Tokio
    RUNTIME.block_on(async {
        let producer = pulsar_arc
            .pulsar
            .producer()
            .with_topic(topic_from_js)
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        /*
        Here will be the point about schema

         schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),

         */

        // return the producer
        Ok(cx.boxed(JsPulsarProducer::new(producer)))
    })
}

fn send_pulsar_message(mut cx: FunctionContext) -> JsResult<JsNull> {
    // get the option on the second optional argument
    let args_obj = cx
        .argument_opt(1)
        .and_then(|a| a.downcast::<JsObject, _>(&mut cx).ok());

    let message_text = get_string_member(&mut cx, args_obj, "message").unwrap(); // Maybe do a better error management
    let payload = message_text.as_bytes().to_vec();
    let m = producer::Message {
        payload,
        ..Default::default()
    };

    // Enter Tokio
    RUNTIME.block_on(async {
        // get the pulsar object
        let producer_arc = Arc::clone(&&cx.argument::<JsBox<Arc<Mutex<JsPulsarProducer>>>>(0)?);

        producer_arc.lock().unwrap().producer.send(m).await
            .unwrap()
            .await
            .unwrap();

        // return the producer
        Ok(cx.null())
    })
}

fn start_pulsar_consumer(mut cx: FunctionContext) -> JsResult<JsNull> {


    // You need to change the return type


    // get the option on the second optional argument
    let args_obj = cx
        .argument_opt(1)
        .and_then(|a| a.downcast::<JsObject, _>(&mut cx).ok());


    // Topic configuration
    let topic_from_js = get_string_member_or_env(&mut cx, args_obj, "topic", "ADDON_PULSAR_TOPIC")
        .unwrap_or_else(|| "non-persistent://public/default/test".to_string());
    let consumer_name_from_js = get_string_member_or_env(
        &mut cx,
        args_obj,
        "consumer_name",
        "ADDON_PULSAR_CONSUMER_NAME",
    )
    .unwrap_or_else(|| "test_consumer".to_string());
    let subscription_name_from_js = get_string_member_or_env(
        &mut cx,
        args_obj,
        "subscription_name",
        "ADDON_PULSAR_SUBSCRIPTION_NAME",
    )
    .unwrap_or_else(|| "test_subscription".to_string());

    let cb = args_obj.unwrap().get(&mut cx, "callback")
        .unwrap()
        .downcast_or_throw::<JsFunction, _>(&mut cx)
        .unwrap().root(&mut cx);

    let channel = cx.channel();

    //let threadedcb = Arc::new(Mutex::new(cb));
    debug!("Topic info for new consumer : {}", topic_from_js);

    let pulsar_arc = Arc::clone(&&cx.argument::<JsBox<Arc<JsPulsar>>>(0)?);

    std::thread::spawn(move || {
    // Enter Tokio
    RUNTIME.block_on(async {


        // get the pulsar object

        let mut consumer: Consumer<String, TokioExecutor> = pulsar_arc
            .pulsar
            .consumer()
            .with_topic(topic_from_js)
            .with_consumer_name(consumer_name_from_js)
            .with_subscription_type(SubType::Exclusive) // To be parametrisable TODO
            .with_subscription(subscription_name_from_js)
            .build()
            .await
            .unwrap();

      //  let jconsumer = JsPulsarConsumer::new(consumer);

    //    let c = Arc::clone(&&jconsumer);

        //   RUNTIME.block_on(async move {

        let mut counter = 0usize;
        let mc = Arc::new(channel);
      //  let mcb = Arc::new(Mutex::new(&cb));
       // x.borrow_mut().s = 6;
//        println!("{}", x.borrow().s);

        while let Some(msg) =  consumer.try_next().await.unwrap() {
            error!("print");
            consumer.ack(&msg).await.unwrap();
            println!("metadata: {:?}", msg.metadata());
            println!("id: {:?}", msg.message_id());
            let data = match msg.deserialize() {
                Ok(data) => {
                   // Arc::clone(&threadedcb).get_mut().unwrap().call(&mut cx, cx.null(), vec![cx.string(data)]).unwrap();
                    Arc::clone(&mc).send( |mut cx| {
                       // let mut ambb = Arc::clone(&mcb);
                        let callback = cb.into_inner(&mut cx);
                        let this = cx.undefined();
                        let null = cx.null();
                        let args = vec![
                            cx.string(data)
                        ];

                        callback.call(&mut cx, this, args).unwrap();
                  //  cb.into_inner(&mut cx).call(&mut cx, cx.null(), vec![]).unwrap();
                        Ok(())
                    });
                },
                Err(e) => {
                    println!("could not deserialize message: {:?}", e);
                    break;
                }
            };


            counter += 1;
            println!("got {} messages", counter);
        }
    })
        //  Ok(());
        //  });

        // return the producer

    });
    Ok(cx.null())
}

// This is useless ATM
fn debug_array_of_objects<'a>(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    //, value:Handle<JsValue>

    // we try to get the object keys to be able to iterate the values
    let object_keys_js: Handle<JsArray> = cx
        .argument::<JsObject>(0)
        .unwrap()
        .get_own_property_names(&mut cx)?;

    // now we need transform again the Handle<JsArray> into some Vec<String> using
    let object_keys_rust: Vec<Handle<JsValue>> = object_keys_js.to_vec(&mut cx)?;

    for key in &object_keys_rust {
        let key_value = key.to_string(&mut cx)?.value(&mut cx);
        let item_value = object_keys_js
            .get(&mut cx, *key)?
            .to_string(&mut cx)?
            .value(&mut cx);
        println!("  {}: {}", key_value, item_value);
    }
    println!("}}");

    Ok(cx.undefined())
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    env_logger::init();

    // Here we bind the functions accessible from the JS (and used by glue code to make sense of it)
    cx.export_function("getPulsar", get_pulsar)?;
    cx.export_function("getPulsarProducer", get_pulsar_producer)?;
    cx.export_function("sendPulsarMessage", send_pulsar_message)?;
    cx.export_function("startPulsarConsumer", start_pulsar_consumer)?;

    cx.export_function("debugArrayOfObjects", debug_array_of_objects)?;

    Ok(())
}
