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

package io.activej.reactor.schedule;

import io.activej.common.time.CurrentTimeProvider;

import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("unused")
public interface ReactorScheduler extends CurrentTimeProvider {
	default ScheduledRunnable schedule(Instant instant, Runnable runnable) {
		return schedule(instant.toEpochMilli(), runnable);
	}

	ScheduledRunnable schedule(long timestamp, Runnable runnable);

	default ScheduledRunnable delay(Duration delay, Runnable runnable) {
		return delay(delay.toMillis(), runnable);
	}

	default ScheduledRunnable delay(long delayMillis, Runnable runnable) {
		return schedule(currentTimeMillis() + delayMillis, runnable);
	}

	default ScheduledRunnable scheduleBackground(Instant instant, Runnable runnable) {
		return scheduleBackground(instant.toEpochMilli(), runnable);
	}

	ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable);

	default ScheduledRunnable delayBackground(Duration delay, Runnable runnable) {
		return delayBackground(delay.toMillis(), runnable);
	}

	default ScheduledRunnable delayBackground(long delayMillis, Runnable runnable) {
		return scheduleBackground(currentTimeMillis() + delayMillis, runnable);
	}
}
