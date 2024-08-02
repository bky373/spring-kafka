/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.kafka.listener;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.listener.AbstractConsumerSeekAwareTests.Config.MultiGroupListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.listener.AbstractConsumerSeekAwareTests.TOTAL_PARTITION_SIZE;

/**
 * @author Borahm Lee
 * @author Artem Bilan
 * @since 3.3
 */
@DirtiesContext
@SpringJUnitConfig
@EmbeddedKafka(topics = {AbstractConsumerSeekAwareTests.TOPIC}, partitions = TOTAL_PARTITION_SIZE)
class AbstractConsumerSeekAwareTests {

	protected static final String TOPIC = "Seek";
	protected static final int TOTAL_PARTITION_SIZE = 9;

	@Autowired
	Config config;

	@Autowired
	KafkaTemplate<String, String> template;

	@Autowired
	MultiGroupListener multiGroupListener;

	@Test
	void seekForAllGroups() throws InterruptedException {
		for (int i = 0; i < TOTAL_PARTITION_SIZE; i++) {
			template.send(TOPIC, i, null, "test-data");
		}

		assertThat(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();

		MultiGroupListener.latch2 = new CountDownLatch(TOTAL_PARTITION_SIZE);

		System.out.println(
				"\n================= Where `ListenerConsumer.seeks` queue is appended. =====================================================");
		System.out.println("## [multiGroupListener.seekToBeginning()] start");
		multiGroupListener.seekToBeginning();
		System.out.println("## [multiGroupListener.seekToBeginning()] end");
		System.out.println(
				"==========================================================================================================================================");
		System.out.println(
				"\n==========================================================================================================================================");
		System.out.println(
				"=========== Before `ListenerConsumer.processSeeks()`, sometimes partitions are newly assigned, BUT `seeks` doesn't change.");
		System.out.println(
				"=========== For this reason, the partition within seeks may not be newly assigned to the consumer, and you may see an error such as `No current assignment for partition Seek-N`.");

		assertThat(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			return factory;
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("test-group", "false", this.broker));
		}

		@Bean
		ProducerFactory<String, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(this.broker));
		}

		@Bean
		KafkaTemplate<String, String> template(ProducerFactory<String, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Component
		static class MultiGroupListener extends AbstractConsumerSeekAware {

			static CountDownLatch latch1 = new CountDownLatch(TOTAL_PARTITION_SIZE);

			static CountDownLatch latch2 = new CountDownLatch(TOTAL_PARTITION_SIZE);

			@KafkaListener(groupId = "group1", topics = TOPIC, concurrency = "5")
			void listenForGroup1(String in) {
				latch1.countDown();
			}

			@KafkaListener(groupId = "group2", topics = TOPIC, concurrency = "7")
			void listenForGroup2(String in) {
				latch2.countDown();
			}

			@EventListener(ConsumerStartedEvent.class)
			public void onApplicationEvent(ConsumerStartedEvent event) {
				System.out.println("## " + event);
			}
		}

	}
}
