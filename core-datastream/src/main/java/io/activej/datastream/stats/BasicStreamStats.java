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

package io.activej.datastream.stats;

import io.activej.common.builder.AbstractBuilder;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;

import java.time.Duration;

public class BasicStreamStats<T> implements StreamStats<T> {
	public static final Duration DEFAULT_BASIC_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final EventStats started = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats resume = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats suspend = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats endOfStream = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final ExceptionStats error = ExceptionStats.create();

	protected BasicStreamStats() {
	}

	public final class Builder extends AbstractStatsBuilder<Builder, BasicStreamStats<T>> {
		Builder() {}
	}

	@SuppressWarnings("unchecked")
	public abstract class AbstractStatsBuilder<Self extends AbstractStatsBuilder<Self, S>, S extends BasicStreamStats<T>>
			extends AbstractBuilder<Self, S> {
		protected AbstractStatsBuilder() {
		}

		public final Self withBasicSmoothingWindow(Duration smoothingWindow) {
			checkNotBuilt(this);
			started.setSmoothingWindow(smoothingWindow);
			resume.setSmoothingWindow(smoothingWindow);
			suspend.setSmoothingWindow(smoothingWindow);
			endOfStream.setSmoothingWindow(smoothingWindow);
			return (Self) this;
		}

		@Override
		protected S doBuild() {
			return (S) BasicStreamStats.this;
		}
	}

	@Override
	public StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor) {
		return actualDataAcceptor;
	}

	@Override
	public void onStarted() {
		started.recordEvent();
	}

	@Override
	public void onResume() {
		resume.recordEvent();
	}

	@Override
	public void onSuspend() {
		suspend.recordEvent();
	}

	@Override
	public void onEndOfStream() {
		endOfStream.recordEvent();
	}

	@Override
	public void onError(Exception e) {
		error.recordException(e);
	}

	@JmxAttribute
	public EventStats getStarted() {
		return started;
	}

	@JmxAttribute
	public EventStats getResume() {
		return resume;
	}

	@JmxAttribute
	public EventStats getSuspend() {
		return suspend;
	}

	@JmxAttribute
	public EventStats getEndOfStream() {
		return endOfStream;
	}

	@JmxAttribute
	public ExceptionStats getError() {
		return error;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActive() {
		return (int) (started.getTotalCount() - (endOfStream.getTotalCount() + error.getTotal()));
	}
}
