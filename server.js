var mosca = require('mosca')
var http = require('http');
var httpServer = http.createServer();
var mongoClient = require('mongodb').MongoClient;


/*var ascoltatore = {
  type: 'mongo',
  url: 'mongodb://localhost:27017/moscamqtt',	// database name
  pubsubCollection: 'mycol',			// collection name
  mongo: {}mm
	// mongo: {
	// 	id: 'admin',
	// 	pwd: 'password'
	// }	
};*/

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

/*httpServer.get('/index.htm', function (req, res) {
   res.sendFile( __dirname + "/" + "index.htm" );
})*/

httpServer.listen(9000);
server.on('ready', setup);
 
// httpServer.use('/', index);    
 


// fired when the mqtt server is ready
function setup() {
	server.authenticate = authenticate;
	server.authorizePublish = authorizePublish;
	server.authorizeSubscribe = authorizeSubscribe;

  	console.log('Mosca server is up and running')
}


/* 
clientInfo collection = {
    clientId: "",
    dateInit: "",
    clientState: "",
    session: {
        startTime: 
        endTime:
    },
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

        var clientInfo = db.collection('clientInfo');
        var infoData = {
            clientState: "connected",
            session: { 
                startTime: Date(),
                endTime: ""
            }
        }

        var newInfoData = {
            clientId: client.id,    // mqtt client ID
            dateInit: Date(),           // the first time device connect to server
            clientState: "connected",   // state of client
            session: {              // sesstion interval
                startTime: Date(),  
                endTime: ""
            }
        }

        // if found: update without "clientId" and "dateInit"
        // if not found: insert new_info_data
        clientInfo.find({clientId: client.id}).count(function(err, count){
            if (err) throw err
            else if (count==0){         // not found
                console.log('count:', count);
                clientInfo.insert(
                    newInfoData,
                    function (err,res) {
                        if (err) throw err;
                        console.log('client connected updateOne-mongodb', res.modifiedCount);
                        db.close();
                });
            } else if (count==1) {      // found
                console.log('count:', count);
                clientInfo.updateOne(
                    { clientId: client.id}, 
                    { $set: infoData},  
                    function (err,res) {
                        if (err) throw err;
                        console.log('client connected updateOne-mongodb', res.modifiedCount);
                        db.close();
                });
            }                
        });
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

            clientInfo.find({clientId: client.id, publishers: {$elemMatch: {topic: packet.topic}}})
            .count(function(err, count) {
                if (err) throw err
                var clientData = db.collection('clientData');
                var publishData = {
                    clientId: client.id,
                    topic: packet.topic,
                    value: packet.payload,
                    timestamp: Date()
                }
                clientData.insertOne(
                    publishData, 
                    function (err, res) {
                        if (err) throw err;
                        else {
                            // not found: insert new "publishers"  (insert object in array)
                            if (count==0) {
                                console.log('count:', count);
                                clientInfo.update(
                                    { clientId: client.id}, 
                                    { $addToSet: { publishers: infoData}}, 
                                    function (err,res) {
                                        if (err) throw err;
                                        console.log('publish info update:');
                                        db.close();
                                });
                            } 
                            // found: update "publishers.state"
                            else if (count==1) {
                                console.log('count:', count);
                                clientInfo.update(
                                    { clientId: client.id, "publishers.topic": packet.topic},
                                    { $set: {"publishers.$.state": infoData.state}},
                                    function(err, res) {
                                console.log('count:', count);
                                        if (err) throw err;
                                        console.log('publish info insert :');
                                        db.close();
                                })
                            }
                        }
                });
            });
        }
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

        clientInfo.find({clientId: client.id, subscribers: {$elemMatch: {topic: topic}}}).count(function(err, count) {
            if (err) throw err
            // not found: insert new "subscribers" (insert object in array)
            else if (count==0) {
                console.log('count:', count);
                clientInfo.update(
                  { clientId: client.id}, 
                  { $addToSet: { subscribers: data}}, 
                  function (err,res) {
                      if (err) throw err;
                      // console.log('Subscribe mongodb:', res);
                      db.close();
                });
            }
            // found: update "subscribers.state" 
            else if (count==1) {
                console.log('count:', count);
                clientInfo.update(
                    { clientId: client.id, "subscribers.topic": topic},
                    { $set: {"subscribers.$.state": data.state}},
                    function(err, res) {
                        if (err) throw err;
                        // console.log('Subscribe mongodb:', res);
                        db.close();
                })
            }

        });

  	});

});
 
// fired when a client subscribes to a topic
server.on('unsubscribed', function(topic, client) {
  	console.log('unsubscribed : ', topic);

    mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
        if (err) throw err;

        var clientInfo = db.collection('clientInfo');
            
        var data = "off"
        
        // 
        clientInfo.update(
            { clientId: client.id, "subscribers.state": "on"}, 
            { $set: {"subscribers.$.state": data}}, 
            function (err,res) {
                if (err) throw err;
                console.log('unsubscribed success :', topic);
                db.close();
        });
    });
});
 
// fired when a client is disconnecting
server.on('clientDisconnecting', function(client) {
  	console.log('clientDisconnecting : ', client.id);
});
 
/* 
fired when a client is disconnected
add "status: disconnected" into collection: clientInfo
 */
server.on('clientDisconnected', function(client) {
  	console.log('clientDisconnected : ', client.id);
    mongoClient.connect('mongodb://127.0.0.1:27017/nthdb', function(err, db) {
        if (err) throw err;

        var clientInfo = db.collection('clientInfo');
                 
        // update "publishers.state" in  "clientInfo"
        var cursor = clientInfo.find({ clientId: client.id, publishers: {$elemMatch: {state: "on"}}});
        
        cursor.forEach(function (doc) {

            doc.session.endTime = Date();

            doc.publishers.forEach(function (publisher) {
                if (publisher.state == "on") {
                    publisher.state="off";
                }
            });
            // update: "disconnected" , "publishers.state"s
            clientInfo.updateOne(
                { clientId: client.id }, 
                { $set: 
                    { clientState: "disconnected", session: doc.session, publishers: doc.publishers}
                },
                function(err, res){
                    if (err) throw err;
                    console.log('clientDisconnected success');
                    db.close();
            });
            
        });            
    });
});