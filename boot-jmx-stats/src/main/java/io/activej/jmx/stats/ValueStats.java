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
import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.common.Preconditions.checkArgument;
import static java.lang.Math.*;
import static java.util.stream.Collectors.toList;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p>
 * Class is supposed to work in single thread
 */
public final class ValueStats implements JmxRefreshableStats<ValueStats>, JmxStatsWithReset, JmxStatsWithSmoothingWindow {
	private static final long MAX_INTERVAL_BETWEEN_REFRESHES = ApplicationSettings.getDuration(JmxStats.class, "maxIntervalBetweenRefreshes", Duration.ofHours(1)).toMillis();
	private static final double LN_2 = log(2);
	private static final String NEG_INF = "-∞";
	private static final String POS_INF = "+∞";

	// endregion

	private long lastTimestampMillis;

	// integer runtime accumulators
	private int lastValueInteger;
	private int lastSumInteger;
	private int lastSqrInteger;
	private int lastCountInteger;
	private int lastMinInteger;
	private int lastMaxInteger;

	// double runtime accumulators
	private double lastValueDouble;
	private double lastSumDouble;
	private double lastSqrDouble;
	private int lastCountDouble;
	private double lastMinDouble = Double.MAX_VALUE;
	private double lastMaxDouble = -Double.MAX_VALUE;

	// calculated during refresh
	private double totalSum;
	private long totalCount;

	// calculated during refresh
	private double smoothedSum;
	private double smoothedSqr;
	private double smoothedCount;
	private double smoothedMin = Double.MAX_VALUE;
	private double smoothedMax = -Double.MAX_VALUE;
	private double absoluteMax = Double.MAX_VALUE;
	private double absoluteMin = -Double.MAX_VALUE;
	private double smoothedRateCount;
	private double smoothedRateTime;

	private double smoothingWindow;
	private double smoothingWindowCoef;

	@Nullable JmxHistogram histogram;

	// fields for aggregation
	private int addedStats;

	// formatting
	@Nullable
	private String unit;
	@Nullable
	private String rateUnit;
	private boolean useAvgAndDeviaton = true;
	private boolean useMinMax = true;
	private boolean useLastValue = true;
	private boolean useAbsoluteValues;
	private int precision = 1000;

	// region builders
	private ValueStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
		resetStats();
	}

	private ValueStats() {
		// create accumulator instance, smoothing window will be taken from actual stats
		this.smoothingWindow = -1;
		this.smoothingWindowCoef = -1;
	}

	public static ValueStats createAccumulator() {
		return new ValueStats();
	}

	/**
	 * Creates new ValueStats with specified smoothing window
	 *
	 * @param smoothingWindow in seconds
	 */
	public static ValueStats create(Duration smoothingWindow) {
		return new ValueStats(smoothingWindow.toMillis() / 1000.0);
	}

	public ValueStats withUnit(String unit) {
		this.unit = unit;
		return this;
	}

	public ValueStats withRate(String rateUnit) {
		this.rateUnit = rateUnit;
		return this;
	}

	public ValueStats withRate() {
		this.rateUnit = "";
		return this;
	}

	public ValueStats withHistogram(JmxHistogram histogram) {
		this.histogram = histogram;
		return this;
	}

	public ValueStats withHistogram(int[] histogram) {
		setHistogram(histogram);
		return this;
	}

	public ValueStats withAbsoluteValues(boolean value) {
		this.useAbsoluteValues = value;
		return this;
	}

	public ValueStats withAverageAndDeviation(boolean value) {
		this.useAvgAndDeviaton = value;
		return this;
	}

	public ValueStats withMinMax(boolean value) {
		this.useMinMax = value;
		return this;
	}

	public ValueStats withLastValue(boolean value) {
		this.useLastValue = value;
		return this;
	}

	public ValueStats withPrecision(int precision) {
		checkArgument(precision > 0, "Precision should be a positive value");
		this.precision = precision;
		return this;
	}

	public ValueStats withScientificNotation() {
		this.precision = -1;
		return this;
	}

	// endregion

	public void setHistogram(JmxHistogram histogram) {
		this.histogram = histogram;
	}

	public void setHistogram(int[] levels) {
		this.histogram = JmxHistogram.ofLevels(levels);
	}

	/**
	 * Resets stats and sets new parameters
	 */
	@Override
	public void resetStats() {
		smoothedSum = 0.0;
		smoothedSqr = 0.0;
		smoothedCount = 0.0;
		smoothedMin = Double.MAX_VALUE;
		smoothedMax = -Double.MAX_VALUE;
		smoothedRateCount = 0;
		smoothedRateTime = 0;

		totalSum = 0.0;
		totalCount = 0;

		absoluteMin = Double.MAX_VALUE;
		absoluteMax = -Double.MAX_VALUE;

		lastMaxInteger = Integer.MIN_VALUE;
		lastMinInteger = Integer.MAX_VALUE;
		lastSumInteger = 0;
		lastSqrInteger = 0;
		lastCountInteger = 0;
		lastValueInteger = 0;

		lastMaxDouble = -Double.MAX_VALUE;
		lastMinDouble = Double.MAX_VALUE;
		lastSumDouble = 0.0;
		lastSqrDouble = 0.0;
		lastCountDouble = 0;
		lastValueDouble = 0.0;

		lastTimestampMillis = 0L;

		if (histogram != null) {
			histogram.reset();
		}
	}

	/**
	 * Adds value
	 */
	public void recordValue(int value) {
		lastValueInteger = value;

		if (value < lastMinInteger) {
			lastMinInteger = value;
		}

		if (value > lastMaxInteger) {
			lastMaxInteger = value;
		}

		lastSumInteger += value;
		lastSqrInteger += value * value;
		lastCountInteger++;

		if (histogram != null) {
			histogram.record(value);
		}
	}

	public void recordValue(double value) {
		lastValueDouble = value;

		if (value < lastMinDouble) {
			lastMinDouble = value;
		}

		if (value > lastMaxDouble) {
			lastMaxDouble = value;
		}

		lastSumDouble += value;
		lastSqrDouble += value * value;
		lastCountDouble++;
	}

	@Override
	public void refresh(long timestamp) {
		double lastSum = 0.0;
		double lastSqr = 0.0;
		long lastCount = 0;
		double lastMin = Double.MAX_VALUE;
		double lastMax = -Double.MAX_VALUE;

		if (lastCountInteger > 0) {
			lastSum += lastSumInteger;
			lastSqr += lastSqrInteger;
			lastCount += lastCountInteger;
			lastMin = lastMinInteger;
			lastMax = lastMaxInteger;

			lastSumInteger = 0;
			lastSqrInteger = 0;
			lastCountInteger = 0;
			lastMinInteger = Integer.MAX_VALUE;
			lastMaxInteger = Integer.MIN_VALUE;
		}

		if (lastCountDouble > 0) {
			lastSum += lastSumDouble;
			lastSqr += lastSqrDouble;
			lastCount += lastCountDouble;
			lastMin = min(lastMin, lastMinDouble);
			lastMax = max(lastMax, lastMaxDouble);

			lastSumDouble = 0;
			lastSqrDouble = 0;
			lastCountDouble = 0;
			lastMinDouble = Double.MAX_VALUE;
			lastMaxDouble = -Double.MAX_VALUE;
		}

		long timeElapsedMillis = lastTimestampMillis == 0L ? 0 : timestamp - lastTimestampMillis;

		if (isTimePeriodValid(timeElapsedMillis)) {
			double timeElapsed = timeElapsedMillis * 0.001;
			double smoothingFactor = exp(timeElapsed * smoothingWindowCoef);

			if (lastCount != 0) {
				smoothedSum = lastSum + smoothedSum * smoothingFactor;
				smoothedSqr = lastSqr + smoothedSqr * smoothingFactor;
				smoothedCount = lastCount + smoothedCount * smoothingFactor;

				totalSum += lastSum;
				totalCount += lastCount;

				double smoothedAvg = smoothedSum / smoothedCount;
				smoothedMin = lastMin < smoothedMin ? lastMin : smoothedAvg + (smoothedMin - smoothedAvg) * smoothingFactor;
				smoothedMax = lastMax > smoothedMax ? lastMax : smoothedAvg + (smoothedMax - smoothedAvg) * smoothingFactor;

				absoluteMin = min(absoluteMin, lastMin);
				absoluteMax = max(absoluteMax, lastMax);
			}

			smoothedRateCount = lastCount + smoothedRateCount * smoothingFactor;
			smoothedRateTime = timeElapsed + smoothedRateTime * smoothingFactor;

		} else {
			// skip stats of last time period
		}

		lastTimestampMillis = timestamp;
	}

	private static boolean isTimePeriodValid(long timePeriod) {
		return timePeriod < MAX_INTERVAL_BETWEEN_REFRESHES && timePeriod >= 0;
	}

	@Override
	public void add(ValueStats anotherStats) {
		if (anotherStats.lastTimestampMillis == 0L)
			return;

		smoothedSum += anotherStats.smoothedSum;
		smoothedSqr += anotherStats.smoothedSqr;
		smoothedCount += anotherStats.smoothedCount;
		smoothedRateCount += anotherStats.smoothedRateCount;
		smoothedRateTime += anotherStats.smoothedRateTime;

		totalSum += anotherStats.totalSum;
		totalCount += anotherStats.totalCount;

		smoothedMin = min(smoothedMin, anotherStats.smoothedMin);
		smoothedMax = max(smoothedMax, anotherStats.smoothedMax);

		if (anotherStats.lastTimestampMillis > lastTimestampMillis) {
			lastTimestampMillis = anotherStats.lastTimestampMillis;
			lastValueInteger = anotherStats.lastValueInteger;
			lastValueDouble = anotherStats.lastValueDouble;
		}

		if (addedStats == 0) {
			smoothingWindow = anotherStats.smoothingWindow;
			smoothingWindowCoef = anotherStats.smoothingWindowCoef;
		} else {
			// all stats should have same smoothing window, -1 means smoothing windows differ in stats, which is error
			if (smoothingWindow != anotherStats.smoothingWindow) {
				smoothingWindow = -1;
				smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
			}
		}

		if (anotherStats.histogram != null) {
			if (this.histogram == null) {
				this.histogram = anotherStats.histogram.createAccumulator();
			}
			this.histogram.add(anotherStats.histogram);
		}

		addedStats++;
	}

	private static double calculateSmoothingWindowCoef(double smoothingWindow) {
		return -(LN_2 / smoothingWindow);
	}

	/**
	 * Returns last added value
	 *
	 * @return last added value
	 */
	@JmxAttribute(optional = true)
	public double getLastValue() {
		return (lastCountInteger > lastCountDouble) ? lastValueInteger : lastValueDouble;
	}

	/**
	 * Returns smoothed average of added values
	 *
	 * @return smoothed average of added values
	 */
	@JmxAttribute(optional = true)
	public double getSmoothedAverage() {
		if (totalCount == 0) {
			return 0.0;
		}

		return smoothedSum / smoothedCount;
	}

	/**
	 * Returns smoothed standard deviation
	 *
	 * @return smoothed standard deviation
	 */
	@JmxAttribute(optional = true)
	public double getSmoothedStandardDeviation() {
		if (totalCount == 0) {
			return 0.0;
		}

		double avg = smoothedSum / smoothedCount;
		double variance = smoothedSqr / smoothedCount - avg * avg;
		if (variance < 0.0)
			variance = 0.0;
		return sqrt(variance);
	}

	/**
	 * Returns smoothed minimum of all added values
	 *
	 * @return smoothed minimum of all added values
	 */
	@JmxAttribute(name = "min", optional = true)
	public double getSmoothedMin() {
		return totalCount == 0 ? 0.0 : smoothedMin;
	}

	/**
	 * Returns smoothed maximum of all added values
	 *
	 * @return smoothed maximum of all added values
	 */
	@JmxAttribute(name = "max", optional = true)
	public double getSmoothedMax() {
		return totalCount == 0 ? 0.0 : smoothedMax;
	}

	/**
	 * Returns minimum of all added values
	 *
	 * @return minimum of all added values
	 */
	@JmxAttribute(name = "absoluteMin", optional = true)
	public double getAbsosuteMin() {
		return totalCount == 0 ? 0.0 : absoluteMin;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	@JmxAttribute(name = "absoluteMax", optional = true)
	public double getAbsoluteMax() {
		return totalCount == 0 ? 0.0 : absoluteMax;
	}

	@JmxAttribute(optional = true)
	public double getAverage() {
		return totalCount != 0L ? totalSum / totalCount : 0.0;
	}

	@JmxAttribute(optional = true)
	public double getSmoothedRate() {
		return totalCount != 0 ? smoothedRateCount / smoothedRateTime * max(1, addedStats) : 0.0;
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

	@JmxAttribute(optional = true)
	public long getCount() {
		return totalCount + lastCountInteger + lastCountDouble;
	}

	@SuppressWarnings({"OptionalGetWithoutIsPresent", "ConstantConditions"})
	@Nullable
	@JmxAttribute(optional = true)
	public List<String> getHistogram() {
		if (histogram == null) {
			return null;
		}

		int[] levels = histogram.levels();
		long[] counts = histogram.counts();
		assert counts.length == levels.length + 1;

		if (Arrays.stream(counts).noneMatch(value -> value != 0)) {
			return null;
		}

		int left = IntStream.range(0, counts.length).filter(i -> (i > 0 && levels[i - 1] == 0) || counts[i] != 0).findFirst().getAsInt();
		int right = IntStream.iterate(levels.length, i -> i - 1).filter(i -> counts[i] != 0).findFirst().getAsInt();

		int maxLevelStrLen = max(max(NEG_INF.length(), POS_INF.length()),
				IntStream.range(left, right).map(i -> Integer.toString(levels[i]).length()).max().orElse(0));
		int maxValueStrLen = IntStream.rangeClosed(left, right).map(i -> Long.toString(counts[i]).length()).max().orElse(0);

		return IntStream.rangeClosed(left, right)
				.mapToObj(i ->
						String.format("%c%" + maxLevelStrLen + "s, %" + maxLevelStrLen + "s%c" + "  :  %" + maxValueStrLen + "s",
								i == 0 ? '(' : '[',
								i == 0 ? NEG_INF : levels[i - 1],
								i == levels.length ? POS_INF : levels[i],
								')',
								counts[i]))
				.collect(toList());
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

		double min = smoothedMin;
		double max = smoothedMax;
		DecimalFormat decimalFormat;

		if (useAbsoluteValues) {
			min = absoluteMin;
			max = absoluteMax;
		}

		if (precision == -1) {
			decimalFormat = new DecimalFormat("0.0####E0#");
		} else {
			decimalFormat = new DecimalFormat("0");
			decimalFormat.setMaximumFractionDigits((int) ceil(min(max(-log10((max - min) / precision), 0), 6)));
		}

		StringBuilder constructorTemplate = new StringBuilder();

		// average and deviation
		if (useAvgAndDeviaton) {
			constructorTemplate
					.append(decimalFormat.format(getSmoothedAverage()))
					.append('±')
					.append(decimalFormat.format(getSmoothedStandardDeviation()))
					.append(' ');
			if (unit != null) {
				constructorTemplate.append(unit)
						.append("  ");
			} else {
				constructorTemplate.append(' ');
			}
		}

		// min and max
		if (useMinMax) {
			constructorTemplate
					.append('[')
					.append(decimalFormat.format(min))
					.append("...")
					.append(decimalFormat.format(max))
					.append("]  ");
		}

		// last value
		if (useLastValue) {
			constructorTemplate
					.append("last: ")
					.append(decimalFormat.format(getLastValue()))
					.append("  ");
		}

		// rate
		if (rateUnit != null) {
			constructorTemplate
					.append("calls: ")
					.append(EventStats.format(totalCount, getSmoothedRate(), rateUnit, decimalFormat))
					.append("  ");
		}

		if (addedStats != 0) {
			constructorTemplate
					.append('[')
					.append(addedStats)
					.append(']');
		}

		return constructorTemplate.toString().trim();
	}
}
