console.log('plop');

var JSONstringify = JSON.stringify;

var cpuCount = require("./index.node");
console.log('plop de milieu');
//console.log(cpuCount.receive());
//console.log(cpuCount.send());
try{
console.log('not url pulsar ',cpuCount.getPulsar({'url':"PLP"}));
}catch(e ){
console.error( "have to fail ", e)
}
var p = cpuCount.getPulsar()
console.log(p);
var prod = cpuCount.getPulsarProducer(p, {})
console.log(prod);

cpuCount.sendPulsarMessage(prod, {message : JSON.stringify({mytitle: 'dfsgdfg', another_field: "fsfsd qsdf qsdf ", andANumber:3})});

cpuCount.startPulsarConsumer(p,{
callback:function(){
console.log(">>>> ",arguments);
}
});


const readline = require('readline');
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

console.log('plop de fin');

var asksend = function(){
rl.question('message to send to pulsar : ', function (m) {
    cpuCount.sendPulsarMessage(prod, {message : m});
    asksend();
});
}

asksend();

rl.on('close', function () {
  console.log('\nBYE BYE !!!');
  process.exit(0);
});


