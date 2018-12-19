"use strict";
/**
 * Created by pedro on 13/Nov/2018.
 */

const Kafka = require("node-rdkafka");
console.log("Consumer starting...");
console.log(`Kafka version: ${Kafka.librdkafkaVersion}`);

const consumer_id = Math.random().toString(36).substring(2, 6) + Math.random().toString(36).substring(2, 6);
console.log(`${consumer_id} - Consumer starting...`);

let msg_last_order=0;
let delay_bucket=[];
let perf_total=0;
let perf_last_total =0;

const perf_mon_start = ()=>{
	setInterval(()=>{
		const avg_delay = delay_bucket.reduce((acc, cur)=>{return acc+cur;}, 0) / delay_bucket.length || 0;

		const perf_mps = perf_total - perf_last_total;
		progress_out(`${consumer_id} - total: ${perf_total} \t last_order: ${msg_last_order} \t messages per sec: ${perf_mps} \t avg delay: ${Math.ceil(avg_delay)}`);

		delay_bucket = [];
		perf_last_total = perf_total;
	}, 1000); // fixed
};

const perf_inc_total = ()=>{
	perf_total++;
};

const perf_record_delay = (delay_ms)=>{
	delay_bucket.push(delay_ms);
};
const perf_record_last_order = (last_order)=>{
	msg_last_order =last_order;
};


const progress_out = (line_str)=>{
	if(process.stdout.clearLine){ // debug doesn't have this
		process.stdout.clearLine();
		process.stdout.cursorTo(0);
		process.stdout.write(line_str);
	}else{
		console.log(line_str);
	}

};


var consumer = new Kafka.KafkaConsumer({
	'group.id': 'kafka',
	'metadata.broker.list': 'localhost:9092',
	// "fetch.wait.max.ms": 1,
	"enable.auto.commit": true,
}, {});


const handler_fn = (err, message)=>{
	if(err)
		return console.error(err);

	if(typeof(messages)==="object"){
		console.log(`${consumer_id} - consumer handler called - single message`);

	}

	if(typeof(messages)==="Array" && messages.length ){
		console.log(`${consumer_id} - consumer handler called - message count: ${messages.length}`);
		// console.log(messages);
	}

};


consumer.connect();

consumer.on('ready', function() {

	console.log(`${consumer_id} - consumer ready, subscribing...`);
	consumer.subscribe(['topic']);

	// Consume from the librdtesting-01 topic. This is what determines
	// the mode we are running in. By not specifying a callback (or specifying
	// only a callback) we get messages as soon as they are available.

}).on('data', function(data) {
	// Output the actual message contents
	// console.log(data.value.toString());
	const msg = JSON.parse(data.value.toString());
	if(!msg || !msg.timestamp)
		return;

	const msg_timestamp = new Date(msg.timestamp);
	const delay = Date.now() - msg_timestamp;

	perf_record_delay(delay);
	perf_record_last_order(msg.order);

	// console.log(`${consumer_id} - Order: ${msg.order}`);
	// console.log(`${consumer_id} - Delay: ${delay}ms`);
	// console.log();

	perf_inc_total();

	consumer.consume(1);

}).on("subscribed", (topics)=>{
	console.log(`${consumer_id} - consumer topic subscription ready`);

	perf_mon_start();

	// consumer.consume(); // flowing mode / stream

	consumer.consume(0);
	// consumer.consume(0, handler_fn);

	console.log(`${consumer_id} - after first consume`);
}).on("event.throttle", ()=>{
	console.log(`${consumer_id} - consumer throttled`);
});

process.on("SIGINT", ()=>{
	if(consumer && consumer.isConnected()){

		consumer.unsubscribe();
		consumer.disconnect();
		console.log(`${consumer_id} - disconnected`);
	}
	process.exit();
});