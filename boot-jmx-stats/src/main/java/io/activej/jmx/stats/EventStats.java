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

package io.activej.jmx.stats;

import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.*;

/**
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p>
 * Class is supposed to work in a single thread
 */
public final class EventStats implements JmxRefreshableStats<EventStats>, JmxStatsWithSmoothingWindow, JmxStatsWithReset {
	private static final DecimalFormatSymbols DECIMAL_FORMAT_SYMBOLS = DecimalFormatSymbols.getInstance(Locale.US);
	private static final long MAX_INTERVAL_BETWEEN_REFRESHES = ApplicationSettings.getDuration(JmxStats.class, "maxIntervalBetweenRefreshes", Duration.ofHours(1)).toMillis();
	private static final double LN_2 = log(2);

	private long lastTimestampMillis;
	private int lastCount;

	private long totalCount;
	private double smoothedRateCount;
	private double smoothedRateTime;

	private double smoothingWindow;
	private double smoothingWindowCoef;

	// fields for aggregation
	private int addedStats;

	// formatting
	private @Nullable String rateUnit;
	private int precision = 1000;

	private EventStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
	}

	private EventStats() {
		// create accumulator instance, smoothing window will be taken from actual stats
		this.smoothingWindow = -1;
		this.smoothingWindowCoef = -1;
	}

	public static EventStats createAccumulator() {
		return new EventStats();
	}

	/**
	 * Creates new EventStats with specified smoothing window
	 *
	 * @param smoothingWindow in seconds
	 */
	public static EventStats create(Duration smoothingWindow) {
		return builder(smoothingWindow).build();
	}

	/**
	 * Creates EventStats builder with specified smoothing window
	 *
	 * @param smoothingWindow in seconds
	 */
	public static Builder builder(Duration smoothingWindow) {
		return new EventStats(smoothingWindow.toMillis() / 1000.0).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, EventStats> {
		private Builder() {}

		public Builder withRateUnit(String rateUnit) {
			checkNotBuilt(this);
			EventStats.this.rateUnit = rateUnit;
			return this;
		}

		public Builder withPrecision(int precision) {
			checkNotBuilt(this);
			checkArgument(precision > 0, "Precision should be a positive value");
			EventStats.this.precision = precision;
			return this;
		}

		public Builder withScientificNotation() {
			checkNotBuilt(this);
			EventStats.this.precision = -1;
			return this;
		}

		@Override
		protected EventStats doBuild() {
			return EventStats.this;
		}
	}

	private static double calculateSmoothingWindowCoef(double smoothingWindow) {
		return -(LN_2 / smoothingWindow);
	}

	/**
	 * Resets rate to zero
	 */
	@Override
	public void resetStats() {
		lastCount = 0;
		totalCount = 0;
		lastTimestampMillis = 0;
		smoothedRateCount = 0;
		smoothedRateTime = 0;
	}

	/**
	 * Records event and updates rate
	 */
	public void recordEvent() {
		lastCount++;
	}

	/**
	 * Records events and updates rate
	 *
	 * @param events number of events
	 */
	public void recordEvents(int events) {
		lastCount += events;
	}

	@Override
	public void refresh(long timestamp) {
		long timeElapsedMillis = timestamp - lastTimestampMillis;

		//noinspection StatementWithEmptyBody
		if (isTimePeriodValid(timeElapsedMillis)) {
			double timeElapsed = timeElapsedMillis * 0.001;
			double smoothingFactor = exp(timeElapsed * smoothingWindowCoef);
			smoothedRateCount = lastCount + smoothedRateCount * smoothingFactor;
			smoothedRateTime = timeElapsed + smoothedRateTime * smoothingFactor;
			totalCount += lastCount;
			lastCount = 0;
		} else {
			// skip stats of last time period
		}

		lastTimestampMillis = timestamp;
	}

	private static boolean isTimePeriodValid(long timePeriod) {
		return timePeriod < MAX_INTERVAL_BETWEEN_REFRESHES && timePeriod >= 0;
	}

	@Override
	public void add(EventStats anotherStats) {
		totalCount += anotherStats.totalCount;
		smoothedRateCount += anotherStats.smoothedRateCount;
		smoothedRateTime += anotherStats.smoothedRateTime;

		if (addedStats == 0) {
			smoothingWindow = anotherStats.smoothingWindow;
			smoothingWindowCoef = anotherStats.smoothingWindowCoef;
			rateUnit = anotherStats.rateUnit;
			precision = anotherStats.precision;
		} else {
			// all stats should have same smoothing window, -1 means smoothing windows differ in stats, which is error
			if (smoothingWindow != anotherStats.smoothingWindow) {
				smoothingWindow = -1;
				smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
			}
			// if rate units differ, use no rate unit
			if (!Objects.equals(rateUnit, anotherStats.rateUnit)) {
				rateUnit = null;
			}
			// if precisions differ, use default precision
			if (precision != anotherStats.precision) {
				precision = 1000;
			}
		}
		addedStats++;
	}

	public static String format(long count, double rate, String rateUnit, DecimalFormat decimalFormat) {
		return count + " @ " + decimalFormat.format(rate) + (rateUnit == null || rateUnit.isEmpty() ? "" : " " + rateUnit) + "/second";
	}

	/**
	 * Returns smoothed value of rate in events per second.
	 * <p>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed value of rate in events per second
	 */
	@JmxAttribute(optional = true)
	public double getSmoothedRate() {
		return totalCount != 0 ? smoothedRateCount / smoothedRateTime * max(1, addedStats) : 0.0;
	}

	/**
	 * Returns total amount of recorded events
	 *
	 * @return total amount of recorded events
	 */
	@JmxAttribute(optional = true)
	public long getTotalCount() {
		return totalCount + lastCount;
	}

	@Override
	@JmxAttribute(optional = true)
	public Duration getSmoothingWindow() {
		return Duration.ofMillis((long) (smoothingWindow * 1000.0));
	}

	@Override
	@JmxAttribute(optional = true)
	public void setSmoothingWindow(Duration smoothingWindow) {
		this.smoothingWindow = smoothingWindow.toMillis() / 1000.0;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(this.smoothingWindow);
	}

	@JmxAttribute
	public String get() {
		return toString();
	}

	@Override
	public String toString() {
		if (totalCount == 0) {
			return "";
		}
		DecimalFormat decimalFormat;
		double smoothedRate = getSmoothedRate();
		if (precision == -1) {
			decimalFormat = new DecimalFormat("0.0####E0#", DECIMAL_FORMAT_SYMBOLS);
		} else {
			decimalFormat = new DecimalFormat("0", DECIMAL_FORMAT_SYMBOLS);
			decimalFormat.setMaximumFractionDigits((int) ceil(min(max(-log10(smoothedRate / precision), 0), 6)));
		}
		String result = format(totalCount, smoothedRate, rateUnit, decimalFormat);
		if (addedStats != 0) {
			result += "  [" + addedStats + ']';
		}
		return result;
	}
}
