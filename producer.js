"use strict";
/**
 * Created by pedro on 13/Nov/2018.
 */

const Kafka = require("node-rdkafka");
console.log("Producer starting...");
console.log(`Kafka version: ${Kafka.librdkafkaVersion}`);


const args = process.argv.slice(2);
const msg_count = args[0]*1 || 100;
const batch_wait_ms = args[1]*1 || 1000;

var producer = new Kafka.Producer({
	'metadata.broker.list': 'localhost:9092',
	"queue.buffering.max.messages": 1000000,
	"batch.num.messages": 100000,
	"queue.buffering.max.ms": 100
});

let last_index=0;

const sender = ()=>{
	for(let i=0; i< msg_count; i++){

		const msg = {timestamp: Date.now(), order: i+last_index};
		// const msg = `Awesome message at ${t}`;
		producer.produce(
			'topic',
			null,
			Buffer.from(JSON.stringify(msg)), // Message to send. Must be a buffer
			null, //msg.timestamp,//'msg_key',
			msg.timestamp // you can send a timestamp here. If your broker version supports it,
		);
		//console.log(`msg '${msg.order}' sent`);
	}
	last_index +=msg_count;

	producer.poll();
};

// Connect to the broker manually
producer.connect();

// Wait for the ready event before proceeding
producer.on('ready', function() {
	try {
		sender();
		setInterval(()=>{
			sender();
		}, batch_wait_ms);
	} catch (err) {
		console.error('A problem occurred when sending our message');
		console.error(err);
	}
});

// Any errors we encounter, including connection errors
producer.on('event.error', function(err) {
	console.error('Error from producer');
	console.error(err);
})