const token  = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkX3NpZyI6Ii1TYXc4cWRYdTVxS01vckFBeVVRQ0FaV3ZMZEJ2UjZtR1FQZUF5Z0pBUlEiLCJwcm9kdWN0X3R5cGUiOiJhY2NvdW50cyIsImxhc3R1cGRhdGV0aW1lIjoiMjAyMS0xMi0xMVQxNjo0MDozNi4xMDMiLCJpc3MiOiJhdmF5YWNsb3VkLmNvbSIsInB1YmxpY2tleWlkIjoiYWd4emZtOXVaWE51WVRJd01UUnlHZ3NTRFVkS2QzUlFkV0pzYVdOTFpYa1lnSUR3ZzdLRnFRa00iLCJleHAiOjE2NDE4MzQ2MjQsInVzZXJfaWQiOiJhZ3h6Zm05dVpYTnVZVEl3TVRSeUVRc1NCRlZ6WlhJWWdJRHdfY2lNb3drTSIsInZlciI6IjIuMCJ9.Qqk-pZWXEXK-7DMLUjU6vI0_VkZFpdi4OQMvbMOq6KgnZ7ZpkJPYxC2GmKpE6hCWCKToxCqeZgv-vsg687KKrxI7xNvxcsQRZHdQnyGHaGOW71hh9tIxChXBYpNqXw7DxfwAvBhX50HseWGltV1f_W-lQ-UGUEWGlK6kMM94c6d9eiFvMEgGFn3Pb6SPCwIjL7M4gBoz57HwY2TDiqXSYTq5uRs3dQQu2JucI09t7jHweROfah25cxqoVqKT0sCJQcgjbt1ZMTLYTLbrZvExVpXxXSmH0BKNBs21Baj5dAtv3AbqkG05pi1-2YOnnL7kAbn48yCvRTPfKLF_ebYfXQ";
const myRoom = "61b4f6d183404d2d08053a4e";
const topic  = 'spaces-' + myRoom;
const kafkaBootstrapServers = 'tadhack-kafka-bootstrap:9092';

const io = require('socket.io-client')
const query = "token=" + token + "=jwt";
const socket = io('https://spacesapis-socket.avayacloud.com/chat', {
      query: query,
      transports: ["websocket"],
      path: "/socket.io",
      hostname: "spacesapis-socket.avayacloud.com",
      secure: true,
      port: "443"
});
const { Kafka } = require('kafkajs');
const readline = require('readline');
var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});
rl.on('line', function(line){
    console.log(line);
    send(myRoom, line)
});

const kafka = new Kafka ({
    clientId: 'spaces',
    brokers: [kafkaBootstrapServers]
});
const producer = kafka.producer();
producer.connect();

function send(room, msg) {
    // topicId = space
    var payload1 = {
        category: 'chat',
        content: {
            bodyText: msg 
        },
        topicId: room,
        loopbackMetadata: 'meta data'
    };
    producer.send({
        topic: topic,
        messages: [{
            key: 'spaces-message', value: JSON.stringify(msg)
        }]
    });
    console.log(JSON.stringify(payload1));
    socket.emit('SEND_MESSAGE', payload1);
}   
function subscribe(room) {  
    // _id = space
    var payload2 = {
        channel: {
            type: 'topic',
            _id: room
        }
    };

    console.log(JSON.stringify(payload2));
    socket.emit('SUBSCRIBE_CHANNEL', payload2);
}

socket.on('CHANNEL_SUBSCRIBED', function() {
    console.log('CHANNEL_SUBSCRIBED');
    // Once channel is sucessfully subscribed to, chats can be sent
    send(myRoom, 'server connected')
});
socket.on('connect', function() {
    console.log("Socket connection success!");
    subscribe(myRoom);
});

socket.on('connect_error', function(error) {
    console.log('Socket connection error: ' + error);
});

socket.on('error', function(error) {
    console.log('Socket error: ' + error);
});

socket.on('disconnect', function() {
    console.log('Socket disconnected.');
});

socket.on('disconnect', function() {
    console.log('Socket disconnected.');
});

socket.on('SEND_MESSAGE_FAILED', function(error) {
    console.log('SEND_MESSAGE_FAILED' + error);
});

socket.on('SUBSCRIBE_CHANNEL_FAILED', function(error) {
    console.log('SUBSCRIBE_CHANNEL_FAILED' + error);
});


socket.on('SEND_MEDIA_SESSION_EVENTS', function() {
    console.log('SEND_MEDIA_SESSION_EVENTS');
});

socket.on('MEDIA_SESSION_RESPONSE', function(msg) {
    console.log('MEDIA_SESSION_RESPONSE');
    console.log("Category = " + msg.category);
});

socket.on('MESSAGE_SENT', function(msg) {
    console.log('MESSAGE_SENT');
    var category = msg.category;
    if (category == "chat") {
        console.log("Message = " + msg.content.bodyText);
    } else {
        console.log("Category = " + category);
    }
});

