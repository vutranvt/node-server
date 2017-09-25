var mosca = require('mosca')
var http = require('http');
var httpServer = http.createServer();
var mongoClient = require('mongodb').MongoClient;
var mongoose = require('mongoose')

mongoose.connect('mongodb://localhost:27017/nthdb');

var User = mongoose.model('User', {clientId: String, type: String, Status: String, 
                            topicPub: Array, topicSub: Array
                        });

var user1 = new User({clientId: 'admin', Status: "connected", });
user1.name = user1.name.toUpperCase();
console.log(user1);

user1.save(function (err, userObj) {
  if (err) {
    console.log(err);
  } else {
    console.log('saved successfully:', userObj);
  }
});


// var ascoltatore = {
//   type: 'mongo',
//   url: 'mongodb://localhost:27017/moscamqtt',	// database name
//   pubsubCollection: 'mycol',			// collection name
//   mongo: {}
// 	// mongo: {
// 	// 	id: 'admin',
// 	// 	pwd: 'password'
// 	// }	
// };

var settings = {
  port: 1884
  // backend: ascoltatore
};

var authenticate = function (client, username, password, callback) {
    if (username == "esp32" && password.toString() == "mtt@23377")
        callback(null, true);
    else
        callback(null, false);
}

var authorizePublish = function (client, topic, payload, callback) {
    var auth = true;
    // set auth to :
    //  true to allow 
    //  false to deny and disconnect
    //  'ignore' to puback but not publish msg.
    callback(null, auth);
}

var authorizeSubscribe = function (client, topic, callback) {
    var auth = true;
    // set auth to :
    //  true to allow
    //  false to deny 
    callback(null, auth);
}

//here we start mosca
var server = new mosca.Server(settings);
server.attachHttpServer(httpServer);
httpServer.listen(9000);
server.on('ready', setup);
 
// fired when the mqtt server is ready
function setup() {
	server.authenticate = authenticate;
	server.authorizePublish = authorizePublish;
	server.authorizeSubscribe = authorizeSubscribe;

  	console.log('Mosca server is up and running')
}
 
// fired whena  client is connected
// add "status: connected" into collection: "clientInfo"
server.on('clientConnected', function(client) {     
  	console.log('client connected', client.id);
});
 
// fired when a message is received
server.on('published', function(packet, client) {
  	console.log('Published : ', JSON.stringify(packet.payload));
});
 
// fired when a client subscribes to a topic
server.on('subscribed', function(topic, client) {
  	console.log('subscribed : ', topic);

// Mongodb Core
  	// mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
  	//     if (err) throw err;
  	//     //use product collection
  	//     var clientInfo = db.collection('clientInfo');
  	//     // var data = JSON.stringify(client.id);
  	//     var data = {
  	//     	clientId: client.id,
  	//     	topicPubSub: topic
  	//     }
   //      // upsert =true: Neu ko tìm thấy dữ liệu filter, thì insert dữ liệu mới vào
  	//     clientInfo.updateOne({clientId: client.id}, {$set: data}, {upsert: true}, function (err,res) {
  	//         //neu xay ra loi
  	//         if (err) throw err;
  	//         //neu khong co loi
  	//         console.log('Update thanh cong');
  	//     });
  	//     db.close();
  	// });

// mongoose framework


});
 
// fired when a client subscribes to a topic
server.on('unsubscribed', function(topic, client) {
  	console.log('unsubscribed : ', topic);
});
 
// fired when a client is disconnecting
server.on('clientDisconnecting', function(client) {
  	console.log('clientDisconnecting : ', client.id);
});
 
// fired when a client is disconnected
// add "status: disconnected" into collection: clientInfo
server.on('clientDisconnected', function(client) {
  	console.log('clientDisconnected : ', client.id);
});