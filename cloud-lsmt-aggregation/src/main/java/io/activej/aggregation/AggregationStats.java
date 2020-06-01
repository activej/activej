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

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public class AggregationStats {
	final StreamStatsBasic<?> mergeMapInput = StreamStats.basic();
	final StreamStatsBasic<?> mergeMapOutput = StreamStats.basic();
	final StreamStatsBasic<?> mergeReducerInput = StreamStats.basic();
	final StreamStatsBasic<?> mergeReducerOutput = StreamStats.basic();

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
