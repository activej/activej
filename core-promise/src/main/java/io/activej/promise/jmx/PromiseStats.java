/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.promise.jmx;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.function.BiConsumerEx;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.JmxHistogram;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;

import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;

/**
 * Allows tracking stats of {@link Promise}s.
 */
public class PromiseStats {
	private @Nullable Eventloop eventloop;

	private int activePromises = 0;
	private long lastStartTimestamp = 0;
	private long lastCompleteTimestamp = 0;
	private final ValueStats duration;
	private final ExceptionStats exceptions = ExceptionStats.create();

	protected PromiseStats(@Nullable Eventloop eventloop, ValueStats duration) {
		this.eventloop = eventloop;
		this.duration = duration;
	}

	public static PromiseStats createMBean(Eventloop eventloop, Duration smoothingWindow) {
		return new PromiseStats(eventloop, ValueStats.create(smoothingWindow));
	}

	public static PromiseStats create(Duration smoothingWindow) {
		return new PromiseStats(null, ValueStats.create(smoothingWindow));
	}

	public PromiseStats withHistogram(int[] levels) {
		setHistogram(levels);
		return this;
	}

	public PromiseStats withHistogram(JmxHistogram histogram) {
		setHistogram(histogram);
		return this;
	}

	public void setHistogram(int[] levels) {
		duration.setHistogram(levels);
	}

	public void setHistogram(JmxHistogram histogram) {
		duration.setHistogram(histogram);
	}

	private long currentTimeMillis() {
		if (eventloop == null) {
			eventloop = getCurrentEventloop();
		}
		return eventloop.currentTimeMillis();
	}

	public <T> AsyncSupplier<T> wrapper(AsyncSupplier<T> callable) {
		return () -> monitor(callable.get());
	}

	public <T> Promise<T> monitor(Promise<T> promise) {
		return promise.whenComplete(recordStats());
	}

	public <T> BiConsumerEx<T, Exception> recordStats() {
		activePromises++;
		long before = currentTimeMillis();
		lastStartTimestamp = before;
		return (value, e) -> {
			activePromises--;
			long now = currentTimeMillis();
			long durationMillis = now - before;
			lastCompleteTimestamp = now;
			duration.recordValue(durationMillis);

			if (e != null) {
				exceptions.recordException(e);
			}
		};
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getActivePromises() {
		return activePromises;
	}

	@JmxAttribute
	public @Nullable Instant getLastStartTime() {
		return lastStartTimestamp != 0L ? Instant.ofEpochMilli(lastStartTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Instant getLastCompleteTime() {
		return lastCompleteTimestamp != 0L ? Instant.ofEpochMilli(lastCompleteTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Duration getCurrentDuration() {
		return activePromises != 0 ? Duration.ofMillis(currentTimeMillis() - lastStartTimestamp) : null;
	}

	@JmxAttribute
	public ValueStats getDuration() {
		return duration;
	}

	@JmxAttribute
	public ExceptionStats getExceptions() {
		return exceptions;
	}

	@Override
	public String toString() {
		return "PromiseStats{" +
				"activePromises=" + activePromises +
				", lastStartTimestamp=" + Instant.ofEpochMilli(lastStartTimestamp) +
				", lastCompleteTimestamp=" + Instant.ofEpochMilli(lastCompleteTimestamp) +
				", duration=" + duration +
				", exceptions=" + exceptions +
				'}';
	}
}
