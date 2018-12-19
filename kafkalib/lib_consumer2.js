"use strict";
/**
 * Created by pedro on 14/Nov/2018.
 */

const Kafka = require("node-rdkafka");
const assert = require("assert");
const EventEmitter = require("events");


class Consumer extends EventEmitter{

	constructor (topics = [], config = {}, handler_fn) {
		super();
		assert.ok(Array.isArray(topics) && topics.length>0, "invalid topics");
		assert.ok(handler_fn && typeof(handler_fn) === "function", "invalid handler_fn");
		assert.ok(config && config.rdkafka && config.rdkafka["metadata.broker.list"] && config.rdkafka["group.id"],
			"invalid config - missing 'config.rdkafka.metadata.broker.list' or 'config.rdkafka.metadata.group.id'");

		this._broker_list = config.rdkafka["metadata.broker.list"];
		this._group_id = config.rdkafka["group.id"];
		this._topics = topics;
		this._handler_fn = handler_fn;

		// const consumer_id = Math.random().toString(36).substring(2, 6) + Math.random().toString(36).substring(2, 6);

		this._consumer = new Kafka.KafkaConsumer({
			// global configs
			"debug": "all",
			// "client.id": "lib_test_"+consumer_id,
			"group.id": this._group_id,
			"metadata.broker.list": this._broker_list,
			"enable.auto.commit": false,
			"offset_commit_cb": this._offset_commit_cb.bind(this),
			"rebalance_cb": this._on_rebalancing_cb.bind(this),
		}, {
			// topic configs
			// "auto.offset.reset": "earliest",
		});


		this._consumer.on('ready', (consumer_info, consumer_metadata)=>{
			this._consumer_info = consumer_info;
			this._consumer_metadata = consumer_metadata;

			this._consumer.subscribe(this._topics);
			// this._consumer.consume(1);
			// this.emit("ready");
		});

		// important events
		this._consumer.on("subscribed", this._on_subscribed.bind(this));
		this._consumer.on('data', this._on_data.bind(this));
		this._consumer.on('event.error', this._on_error.bind(this));

		// other events
		this._consumer.on("event.throttle", ()=>{
			console.log(`consumer throttled`);
		});

		this._consumer.on("disconnected", ()=>{
			console.log(`consumer disconnected`);
		});
		this._consumer.on("event.log", (log)=>{
			// console.log(`consumer log: ${log.fac} - ${log.message}`);
		});


	}

	/**
	 * Connect consumer
	 *
	 * @fires Consumer#ready
	 *
	 * Connects consumer to the Kafka brocker, and sets up the configured processing mode
	 * @return {Promise} - Returns a promise: resolved if successful, or rejection if connection failed
	 */
	connect () {
		this._consumer.connect();
	}

	/**
	 * Disconnect consumer
	 *
	 * Disconnects consumer from the Kafka brocker
	 */
	disconnect (cb = () => {}) {
		this._consumer.unsubscribe();

		this._consumer.disconnect(()=>{
			// disconnected
			cb();
		})
	}

	_on_rebalancing_cb(err, assignment){
		if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
			// Note: this can throw when you are disconnected. Take care and wrap it in
			// a try catch if that matters to you
			// this.assign(assignment);
		} else if (err.code == Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS){
			// Same as above
			// this.unassign();
		} else {
			// We had a real error
			console.error(err);
		}

	}

	_on_subscribed(topics){
		console.log(`consumer topic subscription ready`);

		// TODO: check if the topics we requested are in this array

		// consumer.consume(); // flowing mode / stream

		this._consumer.consume()

		// setInterval(()=>{
		// 	this._consumer.consume(1)
		// }, 1000);
		this.emit("ready");

	}

	_offset_commit_cb(err, topic_partitions){

		if (err) {
			// There was an error committing
			console.error(err);
		} else {
			// Commit went through. Let's log the topic partitions
			console.log(`Commit ok for topic: ${topic_partitions}`);
		}

		this._consumer.consume(10);
	}

	_on_data(data){
		console.log(`Consume on_data - topic: ${data.topic} partition: ${data.partition} offset: ${data.offset}`);

		const msg = JSON.parse(data.value.toString());
		if(!msg || !msg.timestamp)
			return;

		//call handler
		this._handler_fn(msg, (err)=>{
			//if(err) // what to do with returned errors?

			// done, get
			this._consumer.commitMessage(data);
			// this._consumer.consume(1);
		});
	}

	_on_error(err){
		console.error(err);
	}

	/**
	 * This callback returns the message read from Kafka.
	 *
	 * @callback Consumer~workDoneCb
	 * @param {Error} error - An error, if one occurred while reading
	 * the data.
	 * @param {object} messages - Either a list or a single message @see KafkaConsumer~Message
	 * @returns {Promise} - Returns resolved on success, or rejections on failure
	 */

	/**
	 * Consume
	 *
	 * Consume messages from Kafka as per the configuration specified in the constructor.
	 * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
	 */
	consume (workDoneCb) {}

	/**
	 * Commit message in sync mode
	 *
	 * @param {KafkaConsumer~Message} msg - Kafka message to be commited
	 */
	commitMessageSync (msg) {}

}

module.exports.Consumer = Consumer;