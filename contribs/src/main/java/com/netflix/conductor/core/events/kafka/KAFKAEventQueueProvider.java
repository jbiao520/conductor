/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.core.events.kafka;

import com.netflix.conductor.contribs.queue.kafka.KAFKAObservableQueue;
import com.netflix.conductor.contribs.queue.nats.NATSObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import io.nats.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jbiao520
 */
@Singleton
public class KAFKAEventQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(KAFKAEventQueueProvider.class);
    protected Map<String, KAFKAObservableQueue> queues = new ConcurrentHashMap<>();
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    @Inject
    public KAFKAEventQueueProvider(Configuration config) {
        logger.info("NATS Event Queue Provider init");
        
        // Init KAFKA API. Handle "kafka_"  ways to specify parameters
        Properties props = new Properties();
        Properties temp = new Properties();
        temp.putAll(System.getenv());
        temp.putAll(System.getProperties());
        temp.forEach((key1, value) -> {
            String key = key1.toString();
            String val = value.toString();
            
            if (key.startsWith("kafka_")) {
//                key = key.replace("_", ".");
                key = key.split("_")[1];
                props.put(key, config.getProperty(key, val));
            }

        });
        
        // Init NATS API
        consumer = new KafkaConsumer<>(props);
        producer = new KafkaProducer<>(props);
        logger.info("KAFKA Event Queue Provider initialized...");
    }
    
    @Override
    public ObservableQueue getQueue(String queueURI) {
        KAFKAObservableQueue queue = queues.computeIfAbsent(queueURI, q -> new KAFKAObservableQueue(queueURI,consumer,producer));

        return queue;
    }
}
