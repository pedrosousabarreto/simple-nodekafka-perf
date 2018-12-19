"use strict";
/**
 * Created by pedro on 13/Nov/2018.
 */

const Kafka = require("node-rdkafka");
console.log("Stats starting...");
console.log(`Kafka version: ${Kafka.librdkafkaVersion}`);


var consumer = new Kafka.KafkaConsumer({
	'group.id': 'kafka',
	'metadata.broker.list': 'localhost:9092'
}, {});

consumer.connect();

consumer.on('ready', function() {

	console.log(`Stats consumer ready, starting stats...`);

	const meta_req_opts = {topic: "topic", timeout: 800};
	setInterval(()=>{
		consumer.getMetadata(meta_req_opts, (err, meta)=>{
			if(err)
				return console.error(err);

			parse_and_print_meta(meta);

		});


	}, 1000);

}).on("error", (err)=>{
	console.error(`${consumer_id} - consumer error - ${err}`);
});


process.on("SIGINT", ()=>{
	if(consumer && consumer.isConnected()){

		consumer.disconnect();
		console.log(`Stats consumer - disconnected`);
	}
	process.exit();
});

const parse_and_print_meta = (meta)=>{
	const topic_meta = meta.topics.filter(t =>t.name ==="topic");
	if(!topic_meta || topic_meta.length<1)
		return;

	const topic_meta_part = topic_meta[0].partitions;
	console.log(`Partition count: ${topic_meta_part.length}`)

	for(let i=0; i< topic_meta_part.length; i++){
		consumer.queryWatermarkOffsets("topic", i, 500, (err, offsets) =>{
			console.log(`Partition ${i} low: ${offsets.lowOffset} high: ${offsets.highOffset}`);
		});
	}

};