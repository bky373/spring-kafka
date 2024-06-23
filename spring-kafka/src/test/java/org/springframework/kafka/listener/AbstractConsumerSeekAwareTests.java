package org.springframework.kafka.listener;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext
@SpringJUnitConfig
@EmbeddedKafka(topics = {AbstractConsumerSeekAwareTests.TOPIC}, partitions = 1)
public class AbstractConsumerSeekAwareTests {
	static final String TOPIC = "Seek";

	@Autowired
	Config config;

	@Autowired
	Config.MultiGroupListener multiGroupListener;

	@Test
	public void sizeOfCallbacksIsNotSame() {
		// Check the size of registered callbacks
		Map<ConsumerSeekCallback, List<TopicPartition>> callbacksAndTopics = multiGroupListener.getCallbacksAndTopics();
		Set<ConsumerSeekCallback> registeredCallbacks = callbacksAndTopics.keySet();
		assertThat(registeredCallbacks).hasSize(2);

		// Get the size of all seek callbacks
		Map<TopicPartition, ConsumerSeekCallback> topicsToCallback = multiGroupListener.getSeekCallbacks();
		Collection<ConsumerSeekCallback> callbacks = topicsToCallback.values();
		assertThat(callbacks).hasSize(1); // <- I think the result should be two because two callbacks are registered.
	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			return factory;
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("test-group", "false", this.broker));
		}

		@Component
		static class MultiGroupListener extends AbstractConsumerSeekAware {

			@KafkaListener(groupId = "group1", topics = TOPIC)
			void listenForGroup1(String in) {
			}

			@KafkaListener(groupId = "group2", topics = TOPIC)
			void listenForGroup2(String in) {
			}
		}
	}

}
