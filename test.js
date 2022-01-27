var pulsarnative = require('./index.js');

console.log("what's in the package", pulsarnative);
/*
pulsarnative.createApp(function(){
                           console.log(">>>> ",arguments);
                           });
*/
console.log('From native', pulsarnative.sum(40, 2));
var pulsar = pulsarnative.createPulsar();
console.log('From native', pulsar);
var producer = pulsarnative.createPulsarProducer(pulsar);
console.log('From native', producer);
pulsarnative.sendPulsarMessage(producer, {message:"It's a new message"});


pulsarnative.startPulsarConsumer(pulsar, function(){
                                         console.log(">>>> ",arguments);
                                         },{

});



const readline = require('readline');
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

console.log('plop de ready');

var asksend = function(){
rl.question('message to send to pulsar : ', function (m) {
    pulsarnative.sendPulsarMessage(producer, {message : m});
    asksend();
});
}

asksend();

rl.on('close', function () {
  console.log('\nBYE BYE !!!');
  process.exit(0);
});
