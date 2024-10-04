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

package io.activej.eventloop.inspector;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.time.Stopwatch;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerAvg;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.reactor.jmx.ReactiveJmxBean;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;
import static java.lang.Math.pow;

public final class ThrottlingController
	implements ReactiveJmxBean, EventloopInspector {
	private static int staticInstanceCounter = 0;

	private final Logger logger = LoggerFactory.getLogger(ThrottlingController.class.getName() + "." + staticInstanceCounter++);

	public static final Duration TARGET_TIME = Duration.ofMillis(20);
	public static final Duration GC_TIME = Duration.ofMillis(20);
	public static final Duration SMOOTHING_WINDOW = Duration.ofSeconds(10);
	public static final double THROTTLING_DECREASE = 0.1;
	public static final double INITIAL_KEYS_PER_SECOND = 100;
	public static final double INITIAL_THROTTLING = 0.0;

	private Eventloop eventloop;
	private int lastSelectedKeys;
	private int concurrentTasksSize;

	// region settings
	private int targetTimeMillis = (int) TARGET_TIME.toMillis();
	private int gcTimeMillis = (int) GC_TIME.toMillis();
	private double throttlingDecrease = THROTTLING_DECREASE;
	private int smoothingWindow = (int) SMOOTHING_WINDOW.toMillis();
	// endregion

	// region intermediate counters for current round
	private int bufferedRequests;
	private int bufferedRequestsThrottled;
	// endregion

	// region exponentially smoothed values
	private double smoothedThrottling;
	private double smoothedTimePerKeyMillis;
	// endregion

	// region JMX
	private long infoTotalRequests;
	private long infoTotalRequestsThrottled;
	private long infoTotalTimeMillis;
	private long infoRounds;
	private long infoRoundsZeroThrottling;
	private long infoRoundsExceededTargetTime;
	private long infoRoundsGc;
	private long infoGcExceeds;
	// endregion

	private int gcExceededCount;
	private float throttling;

	public static ThrottlingController create() {
		return builder().build();
	}

	public static Builder builder() {
		return new ThrottlingController().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ThrottlingController> {
		private Builder() {}

		public Builder withEventloop(Eventloop eventloop) {
			checkNotBuilt(this);
			setEventloop(eventloop);
			return this;
		}

		public Builder withTargetTime(Duration targetTime) {
			checkNotBuilt(this);
			setTargetTime(targetTime);
			return this;
		}

		public Builder withGcTime(Duration gcTime) {
			checkNotBuilt(this);
			setGcTime(gcTime);
			return this;
		}

		public Builder withSmoothingWindow(Duration smoothingWindow) {
			checkNotBuilt(this);
			setSmoothingWindow(smoothingWindow);
			return this;
		}

		public Builder withThrottlingDecrease(double throttlingDecrease) {
			checkNotBuilt(this);
			setThrottlingDecrease(throttlingDecrease);
			return this;
		}

		public Builder withInitialKeysPerSecond(double initialKeysPerSecond) {
			checkNotBuilt(this);
			checkArgument(initialKeysPerSecond > 0, "Initial keys per second should not be zero or less");
			ThrottlingController.this.smoothedTimePerKeyMillis = 1000.0 / initialKeysPerSecond;
			return this;
		}

		public Builder withInitialThrottling(double initialThrottling) {
			checkNotBuilt(this);
			checkArgument(initialThrottling >= 0, "Initial throttling should not be zero or less");
			ThrottlingController.this.smoothedThrottling = initialThrottling;
			return this;
		}

		@Override
		protected ThrottlingController doBuild() {
			return ThrottlingController.this;
		}
	}

	public void setEventloop(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	private static long rngState = System.nanoTime();

	private static float nextFloat() {
		long x = rngState;
		x ^= (x << 21);
		x ^= (x >>> 35);
		x ^= (x << 4);
		rngState = x;
		x &= ((1L << 24) - 1);
		return (int) x / (float) (1 << 24);
	}

	public boolean isOverloaded() {
		bufferedRequests++;
		if (nextFloat() < throttling) {
			bufferedRequestsThrottled++;
			return true;
		}
		return false;
	}

	@Override
	public void onUpdateConcurrentTasksStats(int concurrentTasksSize, long loopTime) {
		this.concurrentTasksSize = concurrentTasksSize;
	}

	@Override
	public void onUpdateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw, boolean background) {

	}

	@Override
	public void onUpdateScheduledTasksStats(int scheduledTasks, long loopTime, boolean background) {

	}

	@Override
	public void onFatalError(Throwable e, @Nullable Object context) {

	}

	@Override
	public void onScheduledTaskOverdue(long overdue, boolean background) {

	}

	@Override
	public void onUpdateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys, long loopTime) {
		this.lastSelectedKeys = lastSelectedKeys;
	}

	@Override
	public void onUpdateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
	}

	@Override
	public void onUpdateLocalTasksStats(int localTasks, long loopTime) {
	}

	@Override
	public void onUpdateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
	}

	@Override
	public void onUpdateBusinessLogicTime(boolean taskOrKeyPresent, boolean externalTaskPresent, long businessLogicTime) {
		if (businessLogicTime < 0 || businessLogicTime > 60000) {
			logger.warn("Invalid processing time: {}", businessLogicTime);
			return;
		}

		int throttlingKeys = lastSelectedKeys + concurrentTasksSize;
		int lastTimePredicted = (int) (throttlingKeys * smoothedTimePerKeyMillis);
		if (gcTimeMillis != 0.0) {
			if (businessLogicTime > lastTimePredicted + gcTimeMillis) {
				logger.debug("GC detected {} ms, {} keys", businessLogicTime, throttlingKeys);
				businessLogicTime = lastTimePredicted + (long) gcTimeMillis;
				infoRoundsGc++;
				if (++gcExceededCount > 1) {
					infoGcExceeds++;
				}
			} else {
				gcExceededCount = 0;
			}
		}

		double weight = 1.0 - 1.0 / smoothingWindow;

		if (bufferedRequests != 0) {
			assert bufferedRequestsThrottled <= bufferedRequests;
			double value = (double) bufferedRequestsThrottled / bufferedRequests;
			smoothedThrottling = (smoothedThrottling - value) * pow(weight, bufferedRequests) + value;
			infoTotalRequests += bufferedRequests;
			infoTotalRequestsThrottled += bufferedRequestsThrottled;
			bufferedRequests = 0;
			bufferedRequestsThrottled = 0;
		}

		if (throttlingKeys != 0) {
			double value = (double) businessLogicTime / throttlingKeys;
			smoothedTimePerKeyMillis = (smoothedTimePerKeyMillis - value) * pow(weight, throttlingKeys) + value;
		}

		infoTotalTimeMillis += businessLogicTime;
	}

	@Override
	public void onUpdateSelectorSelectTime(long selectorSelectTime) {
		int throttlingKeys = lastSelectedKeys + concurrentTasksSize;
		double predictedTime = throttlingKeys * smoothedTimePerKeyMillis;

		double newThrottling = smoothedThrottling - throttlingDecrease;
		if (newThrottling < 0)
			newThrottling = 0;
		if (predictedTime > targetTimeMillis) {
			double extraThrottling = 1.0 - targetTimeMillis / predictedTime;
			if (extraThrottling > newThrottling) {
				newThrottling = extraThrottling;
				infoRoundsExceededTargetTime++;
			}
		}

		if (newThrottling == 0)
			infoRoundsZeroThrottling++;
		infoRounds++;

		throttling = gcExceededCount >= 2 ? 1.0f : (float) newThrottling;
	}

	@Override
	public void onUpdateSelectorSelectTimeout(long selectorSelectTimeout) {
	}

	@Override
	public void onUpdateSelectedKeyDuration(Stopwatch sw) {
	}

	public double getAvgTimePerKeyMillis() {
		return smoothedTimePerKeyMillis;
	}

	@JmxAttribute(reducer = JmxReducerAvg.class)
	public double getAvgKeysPerSecond() {
		return 1000.0 / smoothedTimePerKeyMillis;
	}

	@JmxAttribute(reducer = JmxReducerAvg.class)
	public double getAvgThrottling() {
		return smoothedThrottling;
	}

	@JmxAttribute
	public Duration getTargetTime() {
		return Duration.ofMillis(targetTimeMillis);
	}

	@JmxAttribute
	public void setTargetTime(Duration targetTime) {
		checkArgument(targetTime.toMillis() > 0, "Target time should not be zero or less");
		this.targetTimeMillis = (int) targetTime.toMillis();
	}

	@JmxAttribute
	public Duration getGcTime() {
		return Duration.ofMillis(gcTimeMillis);
	}

	@JmxAttribute
	public void setGcTime(Duration gcTime) {
		checkArgument(gcTime.toMillis() > 0, "GC time should not be zero or less");
		this.gcTimeMillis = (int) gcTime.toMillis();
	}

	@JmxAttribute
	public double getThrottlingDecrease() {
		return throttlingDecrease;
	}

	@JmxAttribute
	public void setThrottlingDecrease(double throttlingDecrease) {
		checkArgument(throttlingDecrease >= 0.0 && throttlingDecrease <= 1.0, "Throttling decrease should not fall out of [0;1] range");
		this.throttlingDecrease = throttlingDecrease;
	}

	@JmxAttribute
	public Duration getSmoothingWindow() {
		return Duration.ofMillis(smoothingWindow);
	}

	@JmxAttribute
	public void setSmoothingWindow(Duration smoothingWindow) {
		checkArgument(smoothingWindow.toMillis() > 0, "Smoothing window should not be zero or less");
		this.smoothingWindow = (int) smoothingWindow.toMillis();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getTotalRequests() {
		return infoTotalRequests;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getTotalRequestsThrottled() {
		return infoTotalRequestsThrottled;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getTotalProcessed() {
		return infoTotalRequests - infoTotalRequestsThrottled;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getTotalTimeMillis() {
		return infoTotalTimeMillis;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getRounds() {
		return infoRounds;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getRoundsZeroThrottling() {
		return infoRoundsZeroThrottling;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getRoundsExceededTargetTime() {
		return infoRoundsExceededTargetTime;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getInfoRoundsGc() {
		return infoRoundsGc;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getInfoGcExceeds() {
		return infoGcExceeds;
	}

	@JmxAttribute(reducer = JmxReducerAvg.class)
	public double getThrottling() {
		return throttling;
	}

	@JmxOperation
	public void resetInfo() {
		infoTotalRequests = 0;
		infoTotalRequestsThrottled = 0;
		infoTotalTimeMillis = 0;
		infoRounds = 0;
		infoRoundsZeroThrottling = 0;
		infoRoundsExceededTargetTime = 0;
	}

	@Override
	public Eventloop getReactor() {
		return checkNotNull(eventloop);
	}

	@Override
	public String toString() {
		return String.format(
			"{throttling:%2d%% avgKps=%-4d avgThrottling=%2d%% requests=%-4d " +
			"throttled=%-4d rounds=%-3d zero=%-3d >targetTime=%-3d}",
			(int) (throttling * 100),
			(int) getAvgKeysPerSecond(),
			(int) (smoothedThrottling * 100),
			infoTotalRequests,
			infoTotalRequestsThrottled,
			infoRounds,
			infoRoundsZeroThrottling,
			infoRoundsExceededTargetTime);
	}

	@Override
	public <T extends EventloopInspector> @Nullable T lookup(Class<T> type) {
		return null;
	}
}
