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

import io.activej.datastream.stats.BasicStreamStats;
import io.activej.datastream.stats.StreamStats;
import io.activej.jmx.api.attribute.JmxAttribute;

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public class AggregationStats {
	final BasicStreamStats<?> mergeMapInput = StreamStats.basic();
	final BasicStreamStats<?> mergeMapOutput = StreamStats.basic();
	final BasicStreamStats<?> mergeReducerInput = StreamStats.basic();
	final BasicStreamStats<?> mergeReducerOutput = StreamStats.basic();

	@JmxAttribute
	public BasicStreamStats getMergeReducerInput() {
		return mergeReducerInput;
	}

	@JmxAttribute
	public BasicStreamStats getMergeReducerOutput() {
		return mergeReducerOutput;
	}

	@JmxAttribute
	public BasicStreamStats getMergeMapInput() {
		return mergeMapInput;
	}

	@JmxAttribute
	public BasicStreamStats getMergeMapOutput() {
		return mergeMapOutput;
	}
}
