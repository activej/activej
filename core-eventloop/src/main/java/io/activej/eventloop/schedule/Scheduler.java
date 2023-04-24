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

package io.activej.eventloop.schedule;

import io.activej.common.time.CurrentTimeProvider;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("unused")
public interface Scheduler extends CurrentTimeProvider {
	default @NotNull ScheduledRunnable schedule(@NotNull Instant instant, @NotNull Runnable runnable) {
		return schedule(instant.toEpochMilli(), runnable);
	}

	default ScheduledRunnable schedule(long timestamp, Runnable runnable) {
		ScheduledRunnable scheduledRunnable = ScheduledRunnable.of(timestamp, runnable);
		schedule(scheduledRunnable);
		return scheduledRunnable;
	}

	void schedule(ScheduledRunnable scheduledRunnable);

	default @NotNull ScheduledRunnable delay(@NotNull Duration delay, @NotNull Runnable runnable) {
		return delay(delay.toMillis(), runnable);
	}

	default @NotNull ScheduledRunnable delay(long delayMillis, @NotNull Runnable runnable) {
		return schedule(currentTimeMillis() + delayMillis, runnable);
	}

	default @NotNull ScheduledRunnable scheduleBackground(@NotNull Instant instant, @NotNull Runnable runnable) {
		return scheduleBackground(instant.toEpochMilli(), runnable);
	}

	default ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable) {
		ScheduledRunnable scheduledRunnable = ScheduledRunnable.of(timestamp, runnable);
		scheduleBackground(scheduledRunnable);
		return scheduledRunnable;
	}

	void scheduleBackground(ScheduledRunnable scheduledRunnable);

	default @NotNull ScheduledRunnable delayBackground(@NotNull Duration delay, @NotNull Runnable runnable) {
		return delayBackground(delay.toMillis(), runnable);
	}

	default @NotNull ScheduledRunnable delayBackground(long delayMillis, @NotNull Runnable runnable) {
		return scheduleBackground(currentTimeMillis() + delayMillis, runnable);
	}
}
