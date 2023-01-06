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

package io.activej.aggregation;

import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public class AggregationStats extends AbstractReactive {
	final StreamStatsBasic<?> mergeMapInput = StreamStats.basic(reactor);
	final StreamStatsBasic<?> mergeMapOutput = StreamStats.basic(reactor);
	final StreamStatsBasic<?> mergeReducerInput = StreamStats.basic(reactor);
	final StreamStatsBasic<?> mergeReducerOutput = StreamStats.basic(reactor);

	public AggregationStats(Reactor reactor) {
		super(reactor);
	}

	@JmxAttribute
	public StreamStatsBasic getMergeReducerInput() {
		return mergeReducerInput;
	}

	@JmxAttribute
	public StreamStatsBasic getMergeReducerOutput() {
		return mergeReducerOutput;
	}

	@JmxAttribute
	public StreamStatsBasic getMergeMapInput() {
		return mergeMapInput;
	}

	@JmxAttribute
	public StreamStatsBasic getMergeMapOutput() {
		return mergeMapOutput;
	}
}
