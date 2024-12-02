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

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.function.BiConsumerEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.JmxHistogram;
import io.activej.jmx.stats.LongValueStats;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;

import static io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import static io.activej.reactor.Reactor.getCurrentReactor;

/**
 * Allows tracking stats of {@link Promise}s.
 */
public class PromiseStats {
	private @Nullable Reactor reactor;

	private int activePromises = 0;
	private long lastStartTimestamp = 0;
	private long lastCompleteTimestamp = 0;
	private final LongValueStats duration;
	private final ExceptionStats exceptions = ExceptionStats.create();

	protected PromiseStats(@Nullable Reactor reactor, LongValueStats duration) {
		this.reactor = reactor;
		this.duration = duration;
	}

	public static PromiseStats createMBean(Reactor reactor, Duration smoothingWindow) {
		return builder(reactor, smoothingWindow).build();
	}

	public static PromiseStats create(Duration smoothingWindow) {
		return builder(smoothingWindow).build();
	}

	public static Builder builder(Duration smoothingWindow) {
		return new PromiseStats(null, LongValueStats.create(smoothingWindow)).new Builder();
	}

	public static Builder builder(Reactor reactor, Duration smoothingWindow) {
		return new PromiseStats(reactor, LongValueStats.create(smoothingWindow)).new Builder();
	}

	public class Builder extends AbstractBuilder<Builder, PromiseStats> {
		protected Builder() {}

		public final Builder withHistogram(long[] levels) {
			checkNotBuilt(this);
			setHistogram(levels);
			return this;
		}

		public final Builder withHistogram(JmxHistogram histogram) {
			checkNotBuilt(this);
			setHistogram(histogram);
			return this;
		}

		@Override
		protected PromiseStats doBuild() {
			return PromiseStats.this;
		}
	}

	public void setHistogram(long[] levels) {
		duration.setHistogram(levels);
	}

	public void setHistogram(JmxHistogram histogram) {
		duration.setHistogram(histogram);
	}

	private long currentTimeMillis() {
		if (reactor == null) {
			reactor = getCurrentReactor();
		}
		return reactor.currentTimeMillis();
	}

	public <T> AsyncSupplier<T> wrapper(AsyncSupplier<T> supplier) {
		return () -> monitor(supplier.get());
	}

	public AsyncRunnable wrapper(AsyncRunnable runnable) {
		return () -> monitor(runnable.run());
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
	public LongValueStats getDuration() {
		return duration;
	}

	@JmxAttribute
	public ExceptionStats getExceptions() {
		return exceptions;
	}

	@Override
	public String toString() {
		return
			"PromiseStats{" +
			"activePromises=" + activePromises +
			", lastStartTimestamp=" + Instant.ofEpochMilli(lastStartTimestamp) +
			", lastCompleteTimestamp=" + Instant.ofEpochMilli(lastCompleteTimestamp) +
			", duration=" + duration +
			", exceptions=" + exceptions +
			'}';
	}
}
