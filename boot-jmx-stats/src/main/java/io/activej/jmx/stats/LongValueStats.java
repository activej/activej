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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.IntStream;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.*;
import static java.util.stream.Collectors.toList;

/**
 * Counts added values and computes dynamic average using exponential smoothing algorithm
 * <p>
 * Class is supposed to work in a single thread
 */
public final class LongValueStats implements JmxRefreshableStats<LongValueStats>, JmxStatsWithReset, JmxStatsWithSmoothingWindow {
	private static final DecimalFormatSymbols DECIMAL_FORMAT_SYMBOLS = DecimalFormatSymbols.getInstance(Locale.US);
	private static final long MAX_INTERVAL_BETWEEN_REFRESHES = ApplicationSettings.getDuration(JmxStats.class, "maxIntervalBetweenRefreshes", Duration.ofHours(1)).toMillis();
	private static final double LN_2 = log(2);
	private static final String NEG_INF = "-∞";
	private static final String POS_INF = "+∞";

	private long lastTimestampMillis;

	// runtime accumulators
	private long lastValue;
	private long lastSum;
	private long lastSqr;
	private long lastCount;
	private long lastMin;
	private long lastMax;

	// calculated during refresh
	private long totalSum;
	private long totalCount;

	// calculated during refresh
	private double smoothedSum;
	private double smoothedSqr;
	private double smoothedCount;
	private double smoothedMin = Double.MAX_VALUE;
	private double smoothedMax = -Double.MAX_VALUE;
	private long absoluteMax = Long.MAX_VALUE;
	private long absoluteMin = Long.MIN_VALUE;
	private double smoothedRateCount;
	private double smoothedRateTime;

	private double smoothingWindow;
	private double smoothingWindowCoef;

	@Nullable JmxHistogram histogram;

	// fields for aggregation
	private int addedStats;

	// formatting
	private @Nullable String unit;
	private @Nullable String rateUnit;
	private boolean useAvgAndDeviation = true;
	private boolean useMinMax = true;
	private boolean useLastValue = true;
	private boolean useAbsoluteValues;
	private int precision = 1000;

	private LongValueStats(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
		resetStats();
	}

	private LongValueStats() {
		// create accumulator instance, smoothing window will be taken from actual stats
		this.smoothingWindow = -1;
		this.smoothingWindowCoef = -1;
	}

	public static LongValueStats createAccumulator() {
		return accumulatorBuilder().build();
	}

	public static Builder accumulatorBuilder() {
		return new LongValueStats().new Builder();
	}

	/**
	 * Creates new ValueStats with specified smoothing window
	 *
	 * @param smoothingWindow in seconds
	 */
	public static LongValueStats create(Duration smoothingWindow) {
		return builder(smoothingWindow).build();
	}

	/**
	 * Creates ValueStats builder with specified smoothing window
	 *
	 * @param smoothingWindow a smoothing window to use
	 */
	public static Builder builder(Duration smoothingWindow) {
		return new LongValueStats(smoothingWindow.toMillis() / 1000.0).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, LongValueStats> {
		private Builder() {}

		public Builder withUnit(String unit) {
			checkNotBuilt(this);
			LongValueStats.this.unit = unit;
			return this;
		}

		public Builder withRate(String rateUnit) {
			checkNotBuilt(this);
			LongValueStats.this.rateUnit = rateUnit;
			return this;
		}

		public Builder withRate() {
			checkNotBuilt(this);
			LongValueStats.this.rateUnit = "";
			return this;
		}

		public Builder withHistogram(JmxHistogram histogram) {
			checkNotBuilt(this);
			LongValueStats.this.histogram = histogram;
			return this;
		}

		public Builder withHistogram(long[] histogram) {
			checkNotBuilt(this);
			setHistogram(histogram);
			return this;
		}

		public Builder withAbsoluteValues(boolean value) {
			checkNotBuilt(this);
			LongValueStats.this.useAbsoluteValues = value;
			return this;
		}

		public Builder withAverageAndDeviation(boolean value) {
			checkNotBuilt(this);
			LongValueStats.this.useAvgAndDeviation = value;
			return this;
		}

		public Builder withMinMax(boolean value) {
			checkNotBuilt(this);
			LongValueStats.this.useMinMax = value;
			return this;
		}

		public Builder withLastValue(boolean value) {
			checkNotBuilt(this);
			LongValueStats.this.useLastValue = value;
			return this;
		}

		public Builder withPrecision(int precision) {
			checkNotBuilt(this);
			checkArgument(precision > 0, "Precision should be a positive value");
			LongValueStats.this.precision = precision;
			return this;
		}

		public Builder withScientificNotation() {
			checkNotBuilt(this);
			LongValueStats.this.precision = -1;
			return this;
		}

		@Override
		protected LongValueStats doBuild() {
			return LongValueStats.this;
		}
	}

	public void setHistogram(@Nullable JmxHistogram histogram) {
		this.histogram = histogram;
	}

	public void setHistogram(long[] levels) {
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

		totalSum = 0L;
		totalCount = 0L;

		absoluteMin = Long.MAX_VALUE;
		absoluteMax = Long.MIN_VALUE;

		lastMax = Long.MIN_VALUE;
		lastMin = Long.MAX_VALUE;
		lastSum = 0;
		lastSqr = 0;
		lastCount = 0;
		lastValue = 0;

		lastTimestampMillis = 0L;

		if (histogram != null) {
			histogram.reset();
		}
	}

	/**
	 * Adds value
	 */
	public void recordValue(long value) {
		lastValue = value;

		if (value < lastMin) {
			lastMin = value;
		}

		if (value > lastMax) {
			lastMax = value;
		}

		lastSum += value;
		lastSqr += value * value;
		lastCount++;

		if (histogram != null) {
			histogram.record(value);
		}
	}

	@Override
	public void refresh(long timestamp) {
		long lastSum;
		long lastSqr;
		long lastCount;
		long lastMin;
		long lastMax;

		if (this.lastCount > 0) {
			lastSum = this.lastSum;
			lastSqr = this.lastSqr;
			lastCount = this.lastCount;
			lastMin = this.lastMin;
			lastMax = this.lastMax;

			this.lastSum = 0;
			this.lastSqr = 0;
			this.lastCount = 0;
			this.lastMin = Long.MAX_VALUE;
			this.lastMax = Long.MIN_VALUE;
		} else {
			lastSum = 0L;
			lastSqr = 0;
			lastCount = 0;
			lastMin = Long.MAX_VALUE;
			lastMax = Long.MIN_VALUE;
		}

		long timeElapsedMillis = lastTimestampMillis == 0L ? 0 : timestamp - lastTimestampMillis;

		//noinspection StatementWithEmptyBody
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

				absoluteMin = Long.min(absoluteMin, lastMin);
				absoluteMax = Long.max(absoluteMax, lastMax);
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
	public void add(LongValueStats anotherStats) {
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
			lastValue = anotherStats.lastValue;
		}

		if (addedStats == 0) {
			smoothingWindow = anotherStats.smoothingWindow;
			smoothingWindowCoef = anotherStats.smoothingWindowCoef;
			unit = anotherStats.unit;
			rateUnit = anotherStats.rateUnit;
			useAvgAndDeviation = anotherStats.useAvgAndDeviation;
			useMinMax = anotherStats.useMinMax;
			useLastValue = anotherStats.useLastValue;
			useAbsoluteValues = anotherStats.useAbsoluteValues;
		} else {
			// all stats should have same smoothing window, -1 means smoothing windows differ in stats, which is error
			if (smoothingWindow != anotherStats.smoothingWindow) {
				smoothingWindow = -1;
				smoothingWindowCoef = calculateSmoothingWindowCoef(smoothingWindow);
			}
			// if units differ, use no unit
			if (!Objects.equals(unit, anotherStats.unit)) {
				unit = null;
			}
			// if rate units differ, use no rate unit
			if (!Objects.equals(rateUnit, anotherStats.rateUnit)) {
				rateUnit = null;
			}
			// if formatting settings differ, use default one
			useAvgAndDeviation &= anotherStats.useAvgAndDeviation;
			useMinMax &= anotherStats.useMinMax;
			useLastValue &= anotherStats.useLastValue;
			useAbsoluteValues |= anotherStats.useAbsoluteValues;
			// if precisions differ, use default precision
			if (precision != anotherStats.precision) {
				precision = 1000;
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
	public long getLastValue() {
		return lastValue;
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
	public long getAbsoluteMin() {
		return totalCount == 0 ? 0L : absoluteMin;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	@JmxAttribute(name = "absoluteMax", optional = true)
	public long getAbsoluteMax() {
		return totalCount == 0 ? 0L : absoluteMax;
	}

	@JmxAttribute(optional = true)
	public double getAverage() {
		return totalCount != 0L ? (double) totalSum / totalCount : 0.0;
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
		return totalCount + lastCount;
	}

	@SuppressWarnings({"OptionalGetWithoutIsPresent", "ConstantConditions"})
	@JmxAttribute(optional = true)
	public @Nullable List<String> getHistogram() {
		if (histogram == null) {
			return null;
		}

		long[] levels = histogram.levels();
		long[] counts = histogram.counts();
		assert counts.length == levels.length + 1;

		if (Arrays.stream(counts).noneMatch(value -> value != 0)) {
			return null;
		}

		int left = IntStream.range(0, counts.length).filter(i -> (i > 0 && levels[i - 1] == 0) || counts[i] != 0).findFirst().getAsInt();
		int right = IntStream.iterate(levels.length, i -> i - 1).filter(i -> counts[i] != 0).findFirst().getAsInt();

		int maxLevelStrLen = max(max(NEG_INF.length(), POS_INF.length()),
			IntStream.range(left, right).map(i -> Long.toString(levels[i]).length()).max().orElse(0));
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
			decimalFormat = new DecimalFormat("0.0####E0#", DECIMAL_FORMAT_SYMBOLS);
		} else {
			decimalFormat = new DecimalFormat("0", DECIMAL_FORMAT_SYMBOLS);
			decimalFormat.setMaximumFractionDigits((int) ceil(min(max(-log10((max - min) / precision), 0), 6)));
		}

		StringBuilder constructorTemplate = new StringBuilder();

		// average and deviation
		if (useAvgAndDeviation) {
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
				.append(decimalFormat.format(lastValue))
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
