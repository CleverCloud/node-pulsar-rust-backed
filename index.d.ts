/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export class ExternalObject<T> {
  readonly '': {
    readonly '': unique symbol
    [K: symbol]: T
  }
}
export function sum(a: number, b: number): number
/** #[napi(object)] requires all struct fields to be public */
export interface PulsarOptions {
  url?: string | undefined | null
  token?: string | undefined | null
}
/** #[napi(object)] requires all struct fields to be public */
export interface PulsarProducerOptions {
  topic?: string | undefined | null
}
/** #[napi(object)] requires all struct fields to be public */
export interface PulsarMessageOptions {
  message: string
}
/** #[napi(object)] requires all struct fields to be public */
export interface PulsarConsumerOptions {
  topic?: string | undefined | null
  consumerName?: string | undefined | null
  subscriptionName?: string | undefined | null
}
export const enum MessageState {
  ACK = 0,
  NACK = 1
}
export function createPulsar(options?: PulsarOptions | undefined | null): ExternalObject<Arc>
export function createPulsarProducer(pulsar: ExternalObject<Arc>, options?: PulsarProducerOptions | undefined | null): ExternalObject<Arc>
export function sendPulsarMessage(producer: ExternalObject<Arc>, options?: PulsarMessageOptions | undefined | null): null
export function startPulsarConsumer(pulsar: ExternalObject<Arc>, callback: (...args: any[]) => any, options?: PulsarConsumerOptions | undefined | null): ExternalObject<Arc>
export function sendPulsarMessageStatus(consumer: ExternalObject<Arc>, message: ExternalObject<Message>, state: MessageState): void
