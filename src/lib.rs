#![deny(clippy::all)]

use once_cell::sync::Lazy;
use std::env;
use std::sync::{Arc, Mutex};

#[macro_use]
extern crate napi_derive;

use napi::{bindgen_prelude::*, Env};
use pulsar::*;
use tokio::runtime::Runtime;

#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

// We are creating one and only one Tokio runtime, and we will use it everywhere (should work for 99% of usage, and if not, we can invent something another day)
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

#[napi]
fn sum(a: i32, b: i32) -> i32 {
  a + b
}

/// #[napi(object)] requires all struct fields to be public
#[napi(object)]
#[derive(Debug, Clone)]
struct PulsarOptions {
  pub url: Option<String>,
  pub token: Option<String>,
}

/// #[napi(object)] requires all struct fields to be public
#[napi(object)]
#[derive(Debug, Clone)]
struct PulsarProducerOptions {
  pub topic: Option<String>,
}

/// #[napi(object)] requires all struct fields to be public
#[napi(object)]
#[derive(Debug, Clone)]
struct PulsarMessageOptions {
  pub message: String,
}

#[napi]
fn create_pulsar(env: Env, options: Option<PulsarOptions>) -> External<Arc<Pulsar<TokioExecutor>>> {
  let addr_from_js = options
    .clone()
    .map(|o| o.url)
    .flatten()
    .or_else(|| env::var("ADDON_PULSAR_BINARY_URL").ok())
    .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());
  let token_from_js = options
    .clone()
    .map(|o| o.token)
    .flatten()
    .or_else(|| env::var("ADDON_PULSAR_TOKEN").ok());

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
    return External::new(Arc::new(builder.build().await.unwrap()));
  })
}

#[napi]
fn create_pulsar_producer(
  pulsar: External<Arc<Pulsar<TokioExecutor>>>,
  options: Option<PulsarProducerOptions>,
) -> External<Arc<Mutex<Producer<TokioExecutor>>>> {
  let pulsar_arc = Arc::clone(pulsar.as_ref());

  let topic_from_js = options
    .clone()
    .map(|o| o.topic)
    .flatten()
    .or_else(|| env::var("ADDON_PULSAR_TOPIC").ok())
    .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

  debug!("Topic info for new producer : {}", topic_from_js);

  // enter to the tokio thread
  RUNTIME.block_on(async move {
    let producer = pulsar_arc
      .producer()
      .with_topic(topic_from_js)
      .with_name("my producer")
      .with_options(producer::ProducerOptions {
        ..Default::default()
      })
      .build()
      .await
      .unwrap();

    // return the Pulsar object
    return External::new(Arc::new(Mutex::new(producer)));
  })
}

#[napi]
fn send_pulsar_message(
  producer: External<Arc<Mutex<Producer<TokioExecutor>>>>,
  options: Option<PulsarMessageOptions>,
) -> Null {
  let message_text = options.clone().map(|o| o.message).unwrap();
  let payload = message_text.as_bytes().to_vec();
  let m = producer::Message {
    payload,
    ..Default::default()
  };

  // enter to the tokio thread
  RUNTIME.block_on(async move {
    // get the pulsar object
    let producer_arc = Arc::clone(producer.as_ref());

    producer_arc
      .lock()
      .unwrap()
      .send(m)
      .await
      .unwrap()
      .await
      .unwrap();

    // return the Pulsar object
    return Null;
  })
}

#[ctor]
fn foo() {
  env_logger::init();
}
