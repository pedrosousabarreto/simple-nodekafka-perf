"use strict";
/**
 * Created by pedro on 14/Nov/2018.
 */

const Kafka = require("kafka-node");
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
		this._initialised = false;

		// const consumer_id = Math.random().toString(36).substring(2, 6) + Math.random().toString(36).substring(2, 6);

		let consumer_group_options = {
			kafkaHost: this._broker_list,
			// id: consumer_id,
			groupId: this._group_id,
			// sessionTimeout: 15000,
			// An array of partition assignment protocols ordered by preference.
			// 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
			// protocol: ['roundrobin'],
			autoCommit: false,
			fromOffset: "latest", // default is latest
			connectOnReady: true,
			onRebalance: this._on_rebalancing_cb.bind(this)
		};

		this._consumer = new Kafka.ConsumerGroup(consumer_group_options, this._topics);


		this._consumer.on('connect', ()=>{
			if(!this._initialised){
				this._initialised = true;
				this.emit("ready");
			}

			// else -> reconnected

		});

		// important events
		// this._consumer.on("subscribed", this._on_subscribed.bind(this));
		this._consumer.on('message', this._on_data.bind(this));
		// this._consumer.on('event.error', this._on_error.bind(this));

		// other events
		this._consumer.on("ready", ()=>{
			console.log(`consumer throttled`);
		});

		this._consumer.on("done", (topic)=>{
			console.log(`consumer done - ${JSON.stringify(topic)}`);
		});


		// this._consumer.on("disconnected", ()=>{
		// 	console.log(`consumer disconnected`);
		// });
		// this._consumer.on("event.log", (log)=>{
		// 	// console.log(`consumer log: ${log.fac} - ${log.message}`);
		// });


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
		// this._consumer.unsubscribe();

		this._consumer.disconnect(()=>{
			// disconnected
			cb();
		})
	}

	_on_rebalancing_cb(isAlreadyMember, callback){
		if(isAlreadyMember){
			// commit before rebalancing if already part of a group
			return this._consumer.commit(callback);
		}

		callback();
	}

	_commit(message){
		// done, get
		// if(data.partition === 4)
		this._consumer.commit(true, (err, commit_res)=>{

			this._topics.forEach((topic)=>{
				if(commit_res.hasOwnProperty(topic) && commit_res[topic].hasOwnProperty("partition")){
					if(Object.keys(commit_res[topic]).length <=0)
						return;

					const per_topic_commit_res = commit_res[topic];
					if(per_topic_commit_res["errorCode"])
						console.warn(`commited partition ${per_topic_commit_res["partition"]} with errorCode: ${per_topic_commit_res["partition"]}`);
					else
						console.log(`commited partition ${per_topic_commit_res["partition"]}`);
				}
			});

			// if(commit_res[Object.keys(commit_res)[0]].hasOwnProperty("partition")){
			//
			// }
		});
		// this._consumer.consume(1);
	}

	_on_data(data){
		console.log(`Consume on_data - topic: ${data.topic} partition: ${data.partition} offset: ${data.offset}`);
		// this._consumer.pause();

		const msg = JSON.parse(data.value.toString());
		if(!msg || !msg.timestamp)
			return;

		//call handler
		this._handler_fn(msg, (err)=>{
			//if(err) // what to do with returned errors?

			this._commit(msg);
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