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

package io.activej.eventloop.jmx;

import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.StatsUtils;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public interface EventloopJmxBeanEx extends EventloopJmxBean {

	@JmxOperation
	default void resetStats() {
		StatsUtils.resetStats(this);
	}

	@JmxAttribute
	@Nullable
	default Duration getSmoothingWindow() {
		return StatsUtils.getSmoothingWindow(this);
	}

	@JmxAttribute
	default void setSmoothingWindow(Duration smoothingWindowSeconds) {
		StatsUtils.setSmoothingWindow(this, smoothingWindowSeconds);
	}
}
