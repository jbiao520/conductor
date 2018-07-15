/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.contribs.queue.kafka;

import com.amazonaws.services.sqs.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author jbiao520
 *
 */
public class KAFKAObservableQueue implements ObservableQueue {

	private static Logger logger = LoggerFactory.getLogger(KAFKAObservableQueue.class);

	private static final String QUEUE_TYPE = "kafka";

	private String queueURI;
	private String queueName;

	private int pollTimeInMS = 100;
	private KafkaConsumer<String, String> consumer;

	private KafkaProducer<String, String> producer;



	public KAFKAObservableQueue(String queueURI,KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer) {
		this.queueURI = queueURI;
		this.consumer = consumer;
		this.producer = producer;
		logger.info("KAFKAObservableQueue initialized with queueURI="+queueURI);
	}

	@Override
	public Observable<Message> observe() {
		OnSubscribe<Message> subscriber = getOnSubscribe();
		return Observable.create(subscriber);
	}
	@VisibleForTesting
	OnSubscribe<Message> getOnSubscribe() {
		return subscriber -> {
			Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
			interval.flatMap((Long x)->{
				List<Message> msgs = receiveMessages();
				return Observable.from(msgs);
			}).subscribe(subscriber::onNext, subscriber::onError);
		};
	}

	@VisibleForTesting
	List<Message> receiveMessages() {
		try {
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			TopicPartition topicPartition = new TopicPartition(queueURI,1);
			List<ConsumerRecord<String,String>> recordList = records.records(topicPartition);
			for (ConsumerRecord<?, ?> record : records) {
			}
			return null;
		} catch (Exception e) {
			logger.error("Exception while getting messages from SQS ", e);
			Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
		}
		return new ArrayList<>();
	}

	@Override
	public List<String> ack(List<Message> messages) {
		return delete(messages);
	}

	private List<String> delete(List<Message> messages) {
		if (messages == null || messages.isEmpty()) {
			return null;
		}
		DeleteMessageBatchRequest batch = new DeleteMessageBatchRequest().withQueueUrl(queueURI);
		List<DeleteMessageBatchRequestEntry> entries = batch.getEntries();
		messages.forEach(m -> entries.add(new DeleteMessageBatchRequestEntry().withId(m.getId()).withReceiptHandle(m.getReceipt())));
		return null;

	}

	@Override
	public void publish(List<Message> messages) {
		messages.forEach(message -> {
			try {
				String payload = message.getPayload();
				ProducerRecord<String,String> record = new ProducerRecord<String, String>(queueURI,payload);
				producer.send(record);
				logger.info(String.format("Published message to %s: %s", queueURI, payload));
			} catch (Exception ex) {
				logger.error("Failed to publish message " + message.getPayload() + " to " + queueURI, ex);
				throw new RuntimeException(ex);
			}
		});
	}

	@Override
	public long size() {
		return -1;
	}

	@Override
	public void setUnackTimeout(Message message, long unackTimeout) {
		int unackTimeoutInSeconds = (int) (unackTimeout / 1000);
	}

	@Override
	public String getType() {
		return QUEUE_TYPE;
	}

	@Override
	public String getName() {
		return queueName;
	}

	@Override
	public String getURI() {
		return queueURI;
	}

	public static class Builder {
		private String queueURI;

		private KafkaConsumer<String, String> consumer;

		private KafkaProducer<String, String> producer;

		public Builder withqueueURI(String queueURI) {
			this.queueURI = queueURI;
			return this;
		}

		public KAFKAObservableQueue build() {
			return new KAFKAObservableQueue(queueURI, consumer,producer );
		}
	}









}
