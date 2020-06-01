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

import io.activej.datastream.StreamDataAcceptor;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.JmxHistogram;
import io.activej.jmx.stats.ValueStats;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public final class StreamStatsDetailedEx<T> extends StreamStatsBasic<T> {
	public static final Duration DEFAULT_DETAILED_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private final EventStats count = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW).withRateUnit("data items");
	private final ValueStats itemSize = ValueStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private final EventStats totalSize = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);

	@SuppressWarnings("unchecked")
	StreamStatsDetailedEx(StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	@Override
	public StreamStatsDetailedEx<T> withBasicSmoothingWindow(Duration smoothingWindow) {
		return (StreamStatsDetailedEx<T>) super.withBasicSmoothingWindow(smoothingWindow);
	}

	@Override
	public StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor) {
		return sizeCounter == null ?
				new StreamDataAcceptor<T>() {
					final EventStats count = StreamStatsDetailedEx.this.count;

					@Override
					public void accept(T item) {
						count.recordEvent();
						actualDataAcceptor.accept(item);
					}
				} :
				new StreamDataAcceptor<T>() {
					final EventStats count = StreamStatsDetailedEx.this.count;
					final ValueStats itemSize = StreamStatsDetailedEx.this.itemSize;

					@Override
					public void accept(T item) {
						count.recordEvent();
						int size = sizeCounter.size(item);
						itemSize.recordValue(size);
						totalSize.recordEvents(size);
						actualDataAcceptor.accept(item);
					}
				};
	}

	public StreamStatsDetailedEx<T> withHistogram(int[] levels) {
		itemSize.setHistogram(levels);
		return this;
	}

	public StreamStatsDetailedEx<T> withHistogram(JmxHistogram histogram) {
		itemSize.setHistogram(histogram);
		return this;
	}

	@JmxAttribute
	public EventStats getCount() {
		return count;
	}

	@Nullable
	@JmxAttribute
	public ValueStats getItemSize() {
		return sizeCounter != null ? itemSize : null;
	}

	@Nullable
	@JmxAttribute
	public EventStats getTotalSize() {
		return sizeCounter != null ? totalSize : null;
	}

	@Nullable
	@JmxAttribute(reducer = JmxReducerSum.class)
	public Long getTotalSizeAvg() {
		return sizeCounter != null && getStarted().getTotalCount() != 0 ?
				totalSize.getTotalCount() / getStarted().getTotalCount() :
				null;
	}

}
