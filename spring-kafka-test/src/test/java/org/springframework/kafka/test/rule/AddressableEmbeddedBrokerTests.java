/*
 * Copyright 2018-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.rule;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import javax.net.ServerSocketFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class AddressableEmbeddedBrokerTests {

	private static final String TEST_EMBEDDED = "testEmbedded";

	@Autowired
	private Config config;

	@Autowired
	private EmbeddedKafkaZKBroker broker;

	@Test
	public void testKafkaEmbedded() {
		assertThat(broker.getBrokersAsString()).isEqualTo("127.0.0.1:" + this.config.kafkaPort);
		assertThat(broker.getZkPort()).isEqualTo(this.config.zkPort);
		assertThat(broker.getBrokersAsString())
				.isEqualTo(System.getProperty(EmbeddedKafkaZKBroker.SPRING_EMBEDDED_KAFKA_BROKERS));
		assertThat(broker.getZookeeperConnectionString())
				.isEqualTo(System.getProperty(EmbeddedKafkaZKBroker.SPRING_EMBEDDED_ZOOKEEPER_CONNECT));
	}

	@Test
	public void testLateStartedConsumer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(TEST_EMBEDDED, "false", this.broker);
		Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		this.broker.consumeFromAnEmbeddedTopic(consumer, TEST_EMBEDDED);

		Producer<String, Object> producer = new KafkaProducer<>(KafkaTestUtils.producerProps(this.broker));
		producer.send(new ProducerRecord<>(TEST_EMBEDDED, "foo"));
		producer.close();
		KafkaTestUtils.getSingleRecord(consumer, TEST_EMBEDDED);

		consumerProps = KafkaTestUtils.consumerProps("another" + TEST_EMBEDDED, "false", this.broker);
		Consumer<Integer, String> consumer2 = new KafkaConsumer<>(consumerProps);
		this.broker.consumeFromAnEmbeddedTopic(consumer2, TEST_EMBEDDED);
		KafkaTestUtils.getSingleRecord(consumer2, TEST_EMBEDDED);

		consumer.close();
		consumer2.close();
	}

	@Configuration
	public static class Config {

		private int kafkaPort;

		private int zkPort;

		@Bean
		public EmbeddedKafkaZKBroker broker() throws IOException {
			ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0);
			this.kafkaPort = ss.getLocalPort();
			ss.close();
			ss = ServerSocketFactory.getDefault().createServerSocket(0);
			this.zkPort = ss.getLocalPort();
			ss.close();

			return new EmbeddedKafkaZKBroker(1, true, TEST_EMBEDDED)
					.zkPort(this.zkPort)
					.kafkaPorts(this.kafkaPort);
		}

	}

}
