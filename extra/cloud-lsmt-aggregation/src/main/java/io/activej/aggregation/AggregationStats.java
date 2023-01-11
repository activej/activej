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
import io.activej.datastream.stats.StreamStats_Basic;
import io.activej.jmx.api.attribute.JmxAttribute;

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public class AggregationStats {
	final StreamStats_Basic<?> mergeMapInput = StreamStats.basic();
	final StreamStats_Basic<?> mergeMapOutput = StreamStats.basic();
	final StreamStats_Basic<?> mergeReducerInput = StreamStats.basic();
	final StreamStats_Basic<?> mergeReducerOutput = StreamStats.basic();

	@JmxAttribute
	public StreamStats_Basic getMergeReducerInput() {
		return mergeReducerInput;
	}

	@JmxAttribute
	public StreamStats_Basic getMergeReducerOutput() {
		return mergeReducerOutput;
	}

	@JmxAttribute
	public StreamStats_Basic getMergeMapInput() {
		return mergeMapInput;
	}

	@JmxAttribute
	public StreamStats_Basic getMergeMapOutput() {
		return mergeMapOutput;
	}
}
