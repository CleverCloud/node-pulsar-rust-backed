console.log('plop');

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

console.log('plop de fin');
