#![deny(clippy::all)]

use once_cell::sync::Lazy;
use std::env;
use std::sync::{Arc, Mutex};

#[macro_use]
extern crate napi_derive;

use napi::{bindgen_prelude::*, threadsafe_function::{ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode}, Env, CallContext};
use pulsar::*;
use tokio::runtime::Runtime;
/*
// ATM Serde is not used
#[macro_use]
extern crate serde;
 */
#[macro_use]
extern crate log;
use futures::TryStreamExt;
use napi::bindgen_prelude::External;
use pulsar::consumer::Message;


// We are creating one and only one Tokio runtime, and we will use it everywhere (should work for 99% of usage, and if not, we can invent something another day)
static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

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

/// #[napi(object)] requires all struct fields to be public
#[napi(object)]
#[derive(Debug, Clone)]
struct PulsarConsumerOptions {
  pub topic: Option<String>,
  pub consumer_name: Option<String>,
  pub subscription_name: Option<String>,
}


/// #[napi(object)] requires all struct fields to be public
#[napi(object)]
struct JsPulsarMessage {
  pub message: External<Arc<Message<String>>>,
  pub data_string: String,
}


#[napi]
pub enum JsPulsarMessageState {
  ACK,
  NACK,
}

#[napi]
#[allow(dead_code)]
fn create_pulsar(
  _env: Env,
  options: Option<PulsarOptions>,
) -> External<Arc<Pulsar<TokioExecutor>>> {
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
fn delete_pulsar(
  pulsar: External<Arc<Pulsar<TokioExecutor>>>,
) -> Result<()> {
  //let pulsar_arc = Arc::clone(pulsar.as_ref());
  return Err(napi::Error::new(Status::GenericFailure, "not yet implemented, still discussing with pulsar-rs team".to_string()));
}

#[napi]
#[allow(dead_code)]
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
  /*
  TODO error management
  thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: ServiceDiscovery(Query(Some(MetadataError), Some("Namespace not found")))', src/lib.rs:142:8
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
  */
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
fn delete_pulsar_producer(
  pulsar: External<Arc<Pulsar<TokioExecutor>>>,
  producer: External<Arc<Mutex<Producer<TokioExecutor>>>>,
  options: Option<PulsarProducerOptions>,
) -> Result<()> {
  let pulsar_arc = Arc::clone(pulsar.as_ref());


  let producer_arc = Arc::clone(producer.as_ref());


//  return producer_arc.lock().unwrap().close();

  return Err(napi::Error::new(Status::GenericFailure, "not yet implemented, still discussing with pulsar-rs team".to_string()));


}


#[napi]
#[allow(dead_code)]
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
      
    debug!("Send message {}", message_text);
    
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

#[napi]
#[allow(dead_code)]
fn start_pulsar_consumer(
  pulsar: External<Arc<Pulsar<TokioExecutor>>>, //<T: Fn(String) -> Result<()>>
  callback: JsFunction,
  options: Option<PulsarConsumerOptions>,
) -> Result<External<Arc<futures::lock::Mutex<Consumer<String, TokioExecutor>>>>> {
  let topic_from_js = options
    .clone()
    .map(|o| o.topic)
    .flatten()
    .or_else(|| env::var("ADDON_PULSAR_TOPIC").ok())
    .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

  let consumer_name_from_js = options
    .clone()
    .map(|o| o.consumer_name)
    .flatten()
    .or_else(|| env::var("ADDON_PULSAR_CONSUMER_NAME").ok())
    .unwrap_or_else(|| "test_consumer".to_string());

  let subscription_name_from_js = options
    .clone()
    .map(|o| o.subscription_name)
    .flatten()
    .or_else(|| env::var("ADDON_PULSAR_SUBSCRIPTION_NAME").ok())
    .unwrap_or_else(|| "test_subscription".to_string());

  debug!(
    "Topic info for new consumer : {}, sub name : {}, consumer name : {}",
    topic_from_js, subscription_name_from_js, consumer_name_from_js
  );

  let pulsar_arc = Arc::clone(pulsar.as_ref());

  let ts_callback = callback // ThreadsafeFunction<&String, ErrorStrategy::CalleeHandled>
    .create_threadsafe_function(0, |ctx:ThreadSafeCallContext<JsPulsarMessage>| { //ThreadSafeCallContext<String>
      ctx.env.create_object(ctx.value, None).map(|v| vec![v])
    })?;

  // Enter Tokio
  RUNTIME.block_on(async {
    // get the pulsar object

    let consumer: Consumer<String, TokioExecutor> = pulsar_arc
      .consumer()
      .with_topic(topic_from_js)
      .with_consumer_name(consumer_name_from_js)
      .with_subscription_type(SubType::Exclusive) // To be parametrisable TODO
      .with_subscription(subscription_name_from_js)
      .build()
      .await
      .unwrap();

    let consumer_arc = Arc::new(futures::lock::Mutex::new(consumer));

    let my_consumer = consumer_arc.clone();

    RUNTIME.spawn(async move {

      // see https://clevercloud.slack.com/archives/C2ADTSTM4/p1643624845114849
      loop {
        let mut my_consumer = my_consumer.lock().await;

        match my_consumer.try_next().await {
          Ok(maybe_data) => {
            match maybe_data {
              Some(msg) => {
                debug!("New data available");
                debug!("metadata: {:?}", msg.metadata());
                debug!("id: {:?}", msg.message_id());
                let tsfn = ts_callback.clone();
                match msg.deserialize() {
                  // TODO add an error management
                  Ok(data) => {
                    debug!("data:  {}", &data);
                    let jsm = JsPulsarMessage{message :External::new(Arc::new(msg)), data_string:data};

                    ThreadsafeFunction::<External<Arc<JsPulsarMessage>>>::call(&tsfn, Ok(jsm), ThreadsafeFunctionCallMode::Blocking);
                    /*
                    match my_consumer.ack(&msg).await {
                      Ok(_) => {
                          info!("message acked")
                      },
                      _ => {
                        continue
                      }
                    };

                     */
                  }
                  Err(e) => {
                    log::error!("could not deserialize message {:?}", e);
                    break;
                  }
                };
              }, 
              None => break
            }
          },
          Err(_e) => {
            break
          }
        }
      };
    });

    debug!("Pulsar consumer created");

    return Ok(External::new(consumer_arc.clone()));
  })
}

#[napi]
#[allow(dead_code)]
fn send_pulsar_message_status(
  consumer: External<Arc<Mutex<Consumer<String, TokioExecutor>>>>,
  message: External<pulsar::consumer::Message<String>>,
  state: JsPulsarMessageState,
) {
  RUNTIME.block_on(async move {
    // get the pulsar object
    let consumer_arc = Arc::clone(consumer.as_ref());

    let _answer = match state {
      JsPulsarMessageState::ACK => consumer_arc.lock().unwrap().ack(&message).await,
      JsPulsarMessageState::NACK => consumer_arc.lock().unwrap().nack(&message).await,
    };

    // return the Pulsar object
    return ();
  })
}

#[ctor]
fn foo() {
  env_logger::init();
}
