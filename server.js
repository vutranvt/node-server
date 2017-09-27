var mosca = require('mosca')
var http = require('http');
var httpServer = http.createServer();
var mongoClient = require('mongodb').MongoClient;


// //var mongoose = require('mongoose');



// // var uri = 'mongodb://localhost/nthdb';
// // var options = { promiseLibrary: require('bluebird') };
// //var db = mongoose.connection;


// // var db = mongoose.createConnection(uri, options);

// // db.on('error', console.error.bind(console, 'connection error:'));
// db.on('error', console.error);

// db.once('open', function(){
//     console.log('MongoDb connected');
//     //tao schema
//     var clientIdSchema = new mongoose.Schema({
//         clientId: String, 
//         type: String, 
//         Status: String, 
//         topicPub: Array, 
//         topicSub: Array
//     });
//     // tao model
//     var clientId = mongoose.model('clientId', clientIdSchema);

//     client1 =  new clientId({clientId:"123", Status: "connected"});

//     client1.save(function(err, Obj){
//         if (err) {
//             console.log(err);
//         } else {
//             console.log('saved successfully:', Obj);
//         }
//     });

// });


// var ascoltatore = {
//   type: 'mongo',
//   url: 'mongodb://localhost:27017/moscamqtt',	// database name
//   pubsubCollection: 'mycol',			// collection name
//   mongo: {}mm
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


/*
clientInfo collection = {
    clientId: ""
    clientState: ""
    subscribers: [
        {
            topic: "",
            state: ""
        }
    ],
    publishers:[
        {
            topic: "",
            state: ""
        }
    ]
}
*/

// fired whena  client is connected
// add "clientId", "clientStatus"
server.on('clientConnected', function(client) {     
  	console.log('client connected', client.id);

    mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
        if (err) throw err;

        // add "clientId" into "clientInfo" collection
        var clientInfo = db.collection('clientInfo');
        var infoData = {
            clientId: client.id,
            clientState: "connected",
            firstTime: Date(),
            lastTime: ""
        }
        clientInfo.updateOne({clientId: client.id}, {$set: infoData}, {upsert: true}, function (err,res) {
            //neu xay ra loi
            if (err) throw err;
            //neu khong co loi
            console.log('client connected mongodb', res.modifiedCount);
        });

        // add "clientId" into "clientPublish" collection
        var clientPublish = db.collection('clientPublish');
        var pubData = {
            clientId: client.id
        }
        clientPublish.updateOne({clientId: client.id}, {$set: pubData}, {upsert: true}, function (err,res) {
            //neu xay ra loi
            if (err) throw err;
            //neu khong co loi
            console.log('client connected mongodb', res.modifiedCount);
        });    

        db.close();
    });

});
 
// fired when a message is received
server.on('published', function(packet, client) {
  	console.log('Published : ', JSON.stringify(packet.payload));

    mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
        if (err) throw err;

        // add "publishers" into "clientInfo" collection
        if(client){
            var clientInfo = db.collection('clientInfo');
            var infoData =  {
                topic: packet.topic,
                state: "on"
            }
            // update state "publishers.state" in "clientInfo" collection
            clientInfo.update(
                {clientId: client.id, "publishers.topic": packet.topic},
                {$set: {"publishers.$.state": infoData.state}},
                function(err, res) {
                    if (err) throw err;
                    // console.log('Subscribe mongodb:', res);
            })
            // add object in array "publishers"
            clientInfo.update(
                {clientId: client.id}, 
                {$addToSet: {publishers: infoData}}, 
                function (err,res) {
                    if (err) throw err;
                    // console.log('Subscribe mongodb:', res);
            });

            // add publish data to "clientPublish"
            var clientData = db.collection('clientData');
            var pubData = {
                clientId: client.id,
                topic: packet.topic,
                value: packet.payload,
                timestamp: Date()
            }
            clientData.insertOne(
                pubData, 
                function (err,res) {
                    if (err) throw err;
                    // console.log('Subscribe mongodb:', res);
            });
        }

        db.close();
    });
});
 
// fired when a client subscribes to a topic
server.on('subscribed', function(topic, client) {
  	console.log('subscribed topic: ', topic);

    // add "clienId", "status"
  	mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
  	    if (err) throw err;

  	    var clientInfo = db.collection('clientInfo');

  	    var	data =  {
            topic: topic,
            state: "on"
        }
        // delele "subscribers"
        clientInfo.update(
            {clientId: client.id, "subscribers.topic": topic},
            {$set: {"subscribers.$.state": data.state}},
            function(err, res) {
                if (err) throw err;
                // console.log('Subscribe mongodb:', res);
        })
        // add object in array "subscribers"
  	    clientInfo.update(
            {clientId: client.id}, 
            {$addToSet: {subscribers: data}}, 
            function (err,res) {
                if (err) throw err;
                // console.log('Subscribe mongodb:', res);
  	    });

  	    db.close();
  	});

});
 
// fired when a client subscribes to a topic
server.on('unsubscribed', function(topic, client) {
  	//console.log('unsubscribed : ', topic);

    mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
        if (err) throw err;

        var clientInfo = db.collection('clientInfo');
            
        var data = "off"
        
        // upsert =true: Neu ko tìm thấy dữ liệu filter, thì insert dữ liệu mới vào
        clientInfo.update(
            {clientId: client.id, "subscribers.state": "on"}, 
            {$set: {"subscribers.$.state": data}}, 
            {multi: true}, 
            function (err,res) {
                //neu xay ra loi
                if (err) throw err;
                //neu khong co loi
                console.log('unsubscribed success :', topic);
        });

        db.close();
    });
});
 
// fired when a client is disconnecting
server.on('clientDisconnecting', function(client) {
  	console.log('clientDisconnecting : ', client.id);
});
 
// fired when a client is disconnected
// add "status: disconnected" into collection: clientInfo
server.on('clientDisconnected', function(client) {
  	console.log('clientDisconnected : ', client.id);
    mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
        if (err) throw err;

        var clientInfo = db.collection('clientInfo');
            
        var data = {
            clientState: "disconnected",
            lastTime: Date()
            // state: "off"
        }
        
        // update "publishers.state" in  "clientInfo"
        var cursor = clientInfo.find({ clientId: client.id});
        cursor.forEach(function (doc) {
            // if (err) throw err;

            doc.publishers.forEach(function (publisher) {
                  // if (err2) throw err2;
                  if (publisher.state == "on") {
                        publisher.state="off";
                  }

                  console.log('disconnected :', publisher);
            });

            console.log('dis doc: ', doc)
            clientInfo.save(doc, function(err, res){
                if (err) throw err;
                console.log('clientDisconnected success');
            });
          });

        // update "clientState", "lastTime" in "clientInfo"
        clientInfo.updateOne(
            {clientId: client.id}, 
            {$set: data}, 
            function (err,res) {
                if (err) throw err;
                console.log('clientDisconnected success');
        });

        db.close();
    });

});