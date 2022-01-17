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

cpuCount.sendPulsarMessage(prod, {
message : JSON.stringify({mytitle: 'dfsgdfg', another_field: "fsfsd qsdf qsdf ", andANumber:3})})

console.log(cpuCount.debugArrayOfObjects({
    plop:"jkshdfkjh",
    "in":4,
    ret:{
    sdfsdf: "kjldsfhg",
    a:3
    }
}));

console.log('plop de fin');
