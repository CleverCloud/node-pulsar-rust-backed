console.log('plop');

var cpuCount = require("./index.node");
console.log('plop de milieu');
//console.log(cpuCount.receive());
//console.log(cpuCount.send());
try{
console.log(cpuCount.getPulsar({url:"PLP"}));
}catch(e ){
console.error(e)
}
console.log(cpuCount.getPulsar());

console.log('plop de fin');
