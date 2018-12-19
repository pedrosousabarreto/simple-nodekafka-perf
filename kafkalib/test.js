"use strict";
/**
 * Created by pedro on 14/Nov/2018.
 */

const KafkaLib = require(".");

const handler_fn = (msg, done_cb)=>{

	const msg_timestamp = new Date(msg.timestamp);
	const delay = Date.now() - msg_timestamp;

	// simulate async processing
	setTimeout(()=>{
		done_cb();

	},0);
};

const conf ={
	rdkafka:{
		"group.id": "kafka_test2",
		"metadata.broker.list": "localhost:9092"
	}
};

const lib_consumer = new KafkaLib.Consumer(["topic_test"], conf, handler_fn);
lib_consumer.connect();
lib_consumer.on("ready", ()=>{
	console.log("high level ready")
});


process.on("SIGINT", ()=>{
	lib_consumer.disconnect(()=>{
		process.exit();
	});
});