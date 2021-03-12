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

package io.activej.launchers.initializers;

import io.activej.common.jmx.MBeanFormat;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.promise.jmx.PromiseStats;
import io.activej.trigger.TriggerResult;

import java.time.Instant;
import java.util.function.BiPredicate;

public final class TriggersHelper {

	public static TriggerResult ofPromiseStats(PromiseStats promiseStats) {
		ExceptionStats exceptionStats = promiseStats.getExceptions();
		return TriggerResult.ofError(exceptionStats);
	}

	public static TriggerResult ofPromiseStatsLastError(PromiseStats promiseStats) {
		return ofPromiseStats(promiseStats, Instant::isBefore);
	}

	public static TriggerResult ofPromiseStatsLastSuccess(PromiseStats promiseStats) {
		return ofPromiseStats(promiseStats, Instant::isAfter);
	}

	public static TriggerResult ofDelay(long timestamp, long maxDelayMillis) {
		if (timestamp == 0 || System.currentTimeMillis() <= timestamp + maxDelayMillis)
			return TriggerResult.none();
		return TriggerResult.ofValue(MBeanFormat.formatTimestamp(timestamp));
	}

	public static boolean maxDelay(long timestamp, long maxDelayMillis) {
		return timestamp != 0L && System.currentTimeMillis() > timestamp + maxDelayMillis;
	}

	private static TriggerResult ofPromiseStats(PromiseStats promiseStats, BiPredicate<Instant, Instant> completeExceptionTime) {
		ExceptionStats exceptionStats = promiseStats.getExceptions();
		Instant lastExceptionTime = exceptionStats.getLastTime();
		if (lastExceptionTime == null) {
			return TriggerResult.none();
		}
		Instant lastCompleteTime = promiseStats.getLastCompleteTime();
		if (lastCompleteTime == null) {
			return TriggerResult.ofError(exceptionStats);
		}
		return completeExceptionTime.test(lastCompleteTime, lastExceptionTime)
				? TriggerResult.ofError(exceptionStats)
				: TriggerResult.none();
	}
}
