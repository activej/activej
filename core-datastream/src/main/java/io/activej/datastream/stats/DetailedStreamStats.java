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
import io.activej.jmx.stats.JmxStatsWithReset;
import io.activej.jmx.stats.StatsUtils;
import org.jetbrains.annotations.Nullable;

public final class DetailedStreamStats<T> extends BasicStreamStats<T> implements JmxStatsWithReset {
	private @Nullable StreamStatsSizeCounter<T> sizeCounter;

	private long count;
	private long totalSize;

	DetailedStreamStats() {
	}

	public final class Builder extends BasicStreamStats<T>.AbstractStatsBuilder<Builder, DetailedStreamStats<T>> {
		Builder() {}

		public Builder withSizeCounter(StreamStatsSizeCounter<T> sizeCounter) {
			checkNotBuilt(this);
			DetailedStreamStats.this.sizeCounter = sizeCounter;
			return this;
		}
	}

	@Override
	public StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor) {
		return sizeCounter == null ?
				item -> {
					count++;
					actualDataAcceptor.accept(item);
				} :
				item -> {
					count++;
					int size = sizeCounter.size(item);
					totalSize += size;
					actualDataAcceptor.accept(item);
				};
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getCount() {
		return count;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public @Nullable Long getTotalSize() {
		return sizeCounter != null ? totalSize : null;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public @Nullable Long getTotalSizeAvg() {
		return sizeCounter != null && getStarted().getTotalCount() != 0 ?
				totalSize / getStarted().getTotalCount() :
				null;
	}

	@Override
	public void resetStats() {
		count = totalSize = 0;
		StatsUtils.resetStats(this);
	}
}
