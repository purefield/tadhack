const myRoom = "61b4f6d183404d2d08053a4e";
const topic  = 'spaces-' + myRoom;
const clientId = topic + '-sms';
const kafkaBootstrapServers = 'tadhack-kafka-bootstrap:9092';
const request = require('request-promise');

async function send(msg) {
    var url = "https://api-us.cpaas.avayacloud.com/v2/Accounts/AC400001a0ee01c9b648924b68b613b428/SMS/Messages.json?To=+19137084108&From=+16062528425&Body="+encodeURI(msg);
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
    send(line)
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
                send(json.sender + '('+ json.method +')' + ' wrote: ' + json.msg);
            }
        }
        catch (e){
            console.error(e.toString());
        }
    }
});
