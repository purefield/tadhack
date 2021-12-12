const myRoom = "61b4f6d183404d2d08053a4e";
const topic  = 'spaces-' + myRoom;
const clientId = topic + '-sms';
const kafkaBootstrapServers = 'tadhack-kafka-bootstrap:9092';
const request = require('request-promise');
const { htmlToText } = require('html-to-text');

var from = '+16062528425';
var to = ['+9137084108'];
async function send(index, number, msg) {
    await sleep(1500 * index);
    var url = "https://api-us.cpaas.avayacloud.com/v2/Accounts/AC400001a0ee01c9b648924b68b613b428/SMS/Messages.json?To="+encodeURI(number)+"&From="+encodeURI(from)+"&Body="+encodeURI(msg);
    var response = await request.post({
        url: url, 
        auth: {
            "pass": "a14bde7a3f0c431c98cf12967b03fd5b",
            "user": "AC400001a0ee01c9b648924b68b613b428"
        },
        headers : {
            'Accept' : 'application/json', 
            "Content-type": 'application/json'
        }}, function(e , r , body) {});
    var json = JSON.parse(response);
    console.log(response);
}
const readline = require('readline');
var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});
rl.on('line', function(line){
    console.log(line);
    send(0, to[0], line)
});

const { Kafka } = require('kafkajs');
const kafka = new Kafka ({
    clientId: clientId,
    brokers: [kafkaBootstrapServers]
});
const consumer = kafka.consumer({
    groupId: 'sms'
});
consumer.connect();
consumer.subscribe({
    topic: topic,
    fromBeginning: false,
    clientId: clientId
});
consumer.run({
    eachMessage: function ({ topic, partition, message }){
        console.log(JSON.stringify(message.value.toString()));
        try {
            json = JSON.parse(message.value.toString());
            if (json && json.method != 'sms'){
                msg = json.msg;
                const regex = /\#invite\s(?<name>[^\@]*)\@sms\:(?<number>\d*)/;
                const found = msg.match(regex);
                if (found && found.groups){
                    var number = found.groups.number;
                    to.push('+1'+number);
                    console.log('Adding number: ' + number);
                }
                else {
                    to.forEach(function(number, index, array){
                        send(index, number, json.sender + '('+ json.method +')' + ' wrote: ' + htmlToText(json.msg));
                    });
                }
            }
        }
        catch (e){
            console.error(e.toString());
        }
    }
});
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
