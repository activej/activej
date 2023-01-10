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

package io.activej.common.time;

import java.util.concurrent.TimeUnit;

import static io.activej.common.Checks.checkState;
import static java.util.concurrent.TimeUnit.*;

public final class Stopwatch {
	private boolean isRunning;
	private long start;
	private long nanos;

	private Stopwatch() {}

	public static Stopwatch createUnstarted() {return new Stopwatch();}

	public static Stopwatch createStarted() {return new Stopwatch().start();}

	public Stopwatch start() {
		checkState(!isRunning, "This stopwatch is already running.");
		isRunning = true;
		start = System.nanoTime();
		return this;
	}

	public Stopwatch stop() {
		long tick = System.nanoTime();
		checkState(isRunning, "This stopwatch is already stopped.");
		isRunning = false;
		nanos += tick - start;
		return this;
	}

	public Stopwatch reset() {
		isRunning = false;
		nanos = 0;
		return this;
	}

	private long time() {
		if (isRunning) {
			return System.nanoTime() - start + nanos;
		} else {
			return nanos;
		}
	}

	private long elapsedNanos() {
		return isRunning ? System.nanoTime() - start + nanos : nanos;
	}

	@Override
	public String toString() {
		long nanos = elapsedNanos();

		TimeUnit unit = chooseUnit(nanos);
		double value = (double) nanos / NANOSECONDS.convert(1, unit);

		return String.format("%.4g %s", value, abbreviate(unit));
	}

	public long elapsed(TimeUnit timeUnit) {
		return timeUnit.convert(time(), TimeUnit.NANOSECONDS);
	}

	private static TimeUnit chooseUnit(long nanos) {
		if (DAYS.convert(nanos, NANOSECONDS) > 0) {
			return DAYS;
		}
		if (HOURS.convert(nanos, NANOSECONDS) > 0) {
			return HOURS;
		}
		if (MINUTES.convert(nanos, NANOSECONDS) > 0) {
			return MINUTES;
		}
		if (SECONDS.convert(nanos, NANOSECONDS) > 0) {
			return SECONDS;
		}
		if (MILLISECONDS.convert(nanos, NANOSECONDS) > 0) {
			return MILLISECONDS;
		}
		if (MICROSECONDS.convert(nanos, NANOSECONDS) > 0) {
			return MICROSECONDS;
		}
		return NANOSECONDS;
	}

	private static String abbreviate(TimeUnit unit) {
		return switch (unit) {
			case NANOSECONDS -> "ns";
			case MICROSECONDS -> "μs";
			case MILLISECONDS -> "ms";
			case SECONDS -> "s";
			case MINUTES -> "min";
			case HOURS -> "h";
			case DAYS -> "d";
		};
	}
}
