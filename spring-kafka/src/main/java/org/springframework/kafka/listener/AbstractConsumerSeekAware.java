/*
 * Copyright 2019-2024 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.springframework.lang.Nullable;

/**
 * Manages the {@link ConsumerSeekAware.ConsumerSeekCallback} s for the listener. If the
 * listener subclasses this class, it can easily seek arbitrary topics/partitions without
 * having to keep track of the callbacks itself.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public abstract class AbstractConsumerSeekAware implements ConsumerSeekAware {

	private final Map<Thread, ConsumerSeekCallback> callbackForThread = new ConcurrentHashMap<>();

	private final Map<TopicPartition, ConsumerSeekCallback> callbacks = new ConcurrentHashMap<>();

	private final Map<ConsumerSeekCallback, List<TopicPartition>> callbacksToTopic = new ConcurrentHashMap<>();

	/**
	 * BR:
	 * - 스레드를 키로 콜백을 값으로 넣음.
	 * @param callback the callback.
	 */
	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		this.callbackForThread.put(Thread.currentThread(), callback);
	}

	/**
	 * BR:
	 * - 스레드 맵에서 현재 스레드를 키로 주어 콜백을 꺼냄. [스레드(키) -> 콜백(값)]
	 * - 꺼낸 콜백을 토픽 파티션 요소(키)들의 값으로 동일하게 넣어줌. 즉, [토픽 파티션(키) = 콜백(값)]
	 * - 또한, 반대 과정도 수행함. [콜백(키) = 토픽 파티션 목록(값)]
	 * - 콜백 메서드(seekXXX())는 {@link #seekToTimestamp(long)} 가 호출될 때 {@link #getCallbacksAndTopics()} 가 호출되면서 사용된다.
	 * - TODO 알아볼 것. 콜백을 따로 등록하지 않았다면 InitialOrIdleSeekCallback#seekToTimestamp(Collection, long) 가 실행되는 것으로 보임
	 */
	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		ConsumerSeekCallback threadCallback = this.callbackForThread.get(Thread.currentThread());
		if (threadCallback != null) {
			assignments.keySet().forEach(tp -> {
				this.callbacks.put(tp, threadCallback);
				this.callbacksToTopic.computeIfAbsent(threadCallback, key -> new LinkedList<>()).add(tp);
			});
		}
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		partitions.forEach(tp -> {
			ConsumerSeekCallback removed = this.callbacks.remove(tp);
			if (removed != null) {
				List<TopicPartition> topics = this.callbacksToTopic.get(removed);
				if (topics != null) {
					topics.remove(tp);
					if (topics.size() == 0) {
						this.callbacksToTopic.remove(removed);
					}
				}
			}
		});
	}

	@Override
	public void unregisterSeekCallback() {
		this.callbackForThread.remove(Thread.currentThread());
	}

	/**
	 * Return the callback for the specified topic/partition.
	 * @param topicPartition the topic/partition.
	 * @return the callback (or null if there is no assignment).
	 */
	@Nullable
	protected ConsumerSeekCallback getSeekCallbackFor(TopicPartition topicPartition) {
		return this.callbacks.get(topicPartition);
	}

	/**
	 * The map of callbacks for all currently assigned partitions.
	 * @return the map.
	 */
	protected Map<TopicPartition, ConsumerSeekCallback> getSeekCallbacks() {
		return Collections.unmodifiableMap(this.callbacks);
	}

	/**
	 * Return the currently registered callbacks and their associated {@link TopicPartition}(s).
	 * @return the map of callbacks and partitions.
	 * @since 2.6
	 */
	protected Map<ConsumerSeekCallback, List<TopicPartition>> getCallbacksAndTopics() {
		return Collections.unmodifiableMap(this.callbacksToTopic);
	}

	/**
	 * Seek all assigned partitions to the beginning.
	 * @since 2.6
	 */
	public void seekToBeginning() {
		getCallbacksAndTopics().forEach((cb, topics) -> cb.seekToBeginning(topics));
	}

	/**
	 * Seek all assigned partitions to the end.
	 * @since 2.6
	 */
	public void seekToEnd() {
		getCallbacksAndTopics().forEach((cb, topics) -> cb.seekToEnd(topics));
	}

	/**
	 * Seek all assigned partitions to the offset represented by the timestamp.
	 * @param time the time to seek to.
	 * @since 2.6
	 */
	public void seekToTimestamp(long time) {
		getCallbacksAndTopics().forEach((cb, topics) -> cb.seekToTimestamp(topics, time));
	}

}
