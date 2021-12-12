const myRoom = "61b4f6d183404d2d08053a4e";
const topic  = 'spaces-' + myRoom;
const clientId = topic + '-server';
const kafkaBootstrapServers = 'tadhack-kafka-bootstrap:9092';
const request = require('request-promise');

async function getAuthToken(callback) {
    var authData = {
        "displayname": "Bridge",
        "username": "Anonymous"
    };
    var url = "https://spacesapis.avayacloud.com/api/anonymous/auth";
    var response = await request.post({
        url: url, 
        body: JSON.stringify(authData), 
        headers : {
            'Accept' : 'application/json', 
            "Content-type": 'application/json'
        }}, function(e , r , body) {});
    var json = JSON.parse(response);
    token = json.token;
    callback(token);
}
getAuthToken(function(token) {
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
        clientId: clientId,
        brokers: [kafkaBootstrapServers]
    });
    const producer = kafka.producer();
    const consumer = kafka.consumer({
        groupId: 'group'
    });
    producer.connect();
    consumer.connect();
    consumer.subscribe({
        topic: topic,
        fromBeginning: false,
        clientId: clientId
    });
    consumer.run({
        eachMessage: function ({ topic, partition, message }){
            console.log(JSON.stringify(message.value.toString()));
            json = JSON.parse(message.value.toString());
            if (json.method != 'spaces'){
                send(myRoom, json.sender + '('+ json.method +')' + ' wrote: ' + json.msg);
            }
        }
    });

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
    socket.on('MESSAGE_SENT', function(msg) {
        console.log('MESSAGE_SENT');
        var category = msg.category;
        if (category == "chat") {
            console.log("Message = " + JSON.stringify(msg.content));
            if (msg.sender.username != 'Anonymous'){
                producer.send({
                    topic: topic,
                    messages: [{
                        key: 'spaces-message', 
                        value: JSON.stringify({
                            time: msg.content.startTime,
                            sender: msg.sender.displayname,
                            msg: msg.content.bodyText,
                            method: 'spaces'
                        })
                    }]
                });
            }
        } else {
            console.log("Category = " + category);
        }
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
});
