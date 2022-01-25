var pulsarnative = require('./index.js');

console.log('From native', pulsarnative.sum(40, 2));
var pulsar = pulsarnative.createPulsar();
console.log('From native', pulsar);
var producer = pulsarnative.createPulsarProducer(pulsar);
console.log('From native', producer);
pulsarnative.sendPulsarMessage(producer, {message:"It's a new message"});