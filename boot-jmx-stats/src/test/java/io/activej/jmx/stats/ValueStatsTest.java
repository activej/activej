package io.activej.jmx.stats;

import org.junit.Test;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static io.activej.jmx.stats.JmxHistogram.POWERS_OF_TWO;
import static java.lang.Math.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ValueStatsTest {
	private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private static final int ONE_SECOND_IN_MILLIS = 1000;
	private static final Random RANDOM = new Random();

	@Test
	public void smoothedAverageAtLimitShouldBeSameAsInputInCaseOfConstantData() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(smoothingWindow);
		int inputValue = 5;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
		}

		double acceptableError = 10E-5;
		assertEquals(inputValue, valueStats.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void ifDataIsFloatValueStatsShouldApproximateThat() {
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10));
		int inputValue = 5;
		int iterations = 1000;
		int refreshPeriod = ONE_SECOND_IN_MILLIS;
		double acceptableError = 10E-2;

		for (int i = 0; i < iterations; i++) {
			valueStats.refresh(currentTimestamp);
			currentTimestamp += refreshPeriod;
		}
		assertEquals(0, valueStats.getSmoothedAverage(), acceptableError);

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue++);
			valueStats.refresh(currentTimestamp);
			currentTimestamp += refreshPeriod;
		}
		assertEquals(990, valueStats.getSmoothedAverage(), acceptableError);

		for (int i = 0; i < iterations; i++) {
			valueStats.refresh(currentTimestamp);
			currentTimestamp += refreshPeriod;
		}
		assertEquals(990, valueStats.getSmoothedAverage(), acceptableError);

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue++);
			valueStats.refresh(currentTimestamp);
			currentTimestamp += refreshPeriod;
		}
		assertEquals(1990, valueStats.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void itShouldReturnProperStandardDeviationAtLimit() {
		Duration smoothingWindow = Duration.ofSeconds(100);
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(smoothingWindow);
		int iterations = 10000;
		int minValue = 0;
		int maxValue = 10;
		int refreshPeriod = 100;

		for (int i = 0; i < iterations; i++) {
			int currentValue = uniformRandom(minValue, maxValue);
			valueStats.recordValue(currentValue);
			currentTimestamp += refreshPeriod;
			valueStats.refresh(currentTimestamp);
		}

		// standard deviation of uniform distribution
		double expectedStandardDeviation = sqrt(((maxValue - minValue + 1) * (maxValue - minValue + 1) - 1) / 12.0);
		double acceptableError = 0.1;
		assertEquals(expectedStandardDeviation, valueStats.getSmoothedStandardDeviation(), acceptableError);
	}

	@Test
	public void itShouldResetStatsAfterResetMethodCall() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(smoothingWindow);
		int inputValue = 5;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
		}

		double avgBeforeReset = valueStats.getSmoothedAverage();
		valueStats.resetStats();
		double avgAfterReset = valueStats.getSmoothedAverage();

		double acceptableError = 10E-5;
		assertEquals(inputValue, avgBeforeReset, acceptableError);
		assertEquals(0.0, avgAfterReset, acceptableError);
	}

	@Test
	public void itShouldAccumulateProperly() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		long currentTimestamp = 0;
		ValueStats valueStats_1 = ValueStats.create(smoothingWindow);
		ValueStats valueStats_2 = ValueStats.create(smoothingWindow);
		int inputValue_1 = 5;
		int inputValue_2 = 10;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			valueStats_1.recordValue(inputValue_1);
			valueStats_2.recordValue(inputValue_2);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats_1.refresh(currentTimestamp);
			valueStats_2.refresh(currentTimestamp);
		}

		ValueStats accumulator = ValueStats.createAccumulator();
		accumulator.add(valueStats_1);
		accumulator.add(valueStats_2);

		double acceptableError = 10E-5;
		double expectedAccumulatedSmoothedAvg = (5 + 10) / 2.0;
		assertEquals(expectedAccumulatedSmoothedAvg, accumulator.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void itShouldAccumulateWithOriginalUnits() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		long currentTimestamp = 0;
		ValueStats valueStats_1 = ValueStats.builder(smoothingWindow).withUnit("seconds").build();
		ValueStats valueStats_2 = ValueStats.builder(smoothingWindow).withUnit("seconds").build();
		int inputValue_1 = 5;
		int inputValue_2 = 10;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			valueStats_1.recordValue(inputValue_1);
			valueStats_2.recordValue(inputValue_2);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats_1.refresh(currentTimestamp);
			valueStats_2.refresh(currentTimestamp);
		}

		ValueStats accumulator = ValueStats.createAccumulator();
		accumulator.add(valueStats_1);
		accumulator.add(valueStats_2);

		assertThat(accumulator.toString(), containsString("seconds"));
	}

	@Test
	public void itShouldBuildHistogram() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 15, 500})
			.build();

		// first interval
		stats.recordValue(2);
		stats.recordValue(3);
		stats.recordValue(-5);
		stats.recordValue(0);

		// second interval
		stats.recordValue(10);
		stats.recordValue(5);
		stats.recordValue(14);

		// no data for third interval

		// fourth interval
		stats.recordValue(600);
		stats.recordValue(1000);

		// first interval
		for (int i = 0; i < 10; i++) {
			stats.recordValue(1);
		}

		List<String> expected = List.of(
			"( -∞,   5)  :  14",
			"[  5,  15)  :   3",
			"[ 15, 500)  :   0",
			"[500,  +∞)  :   2"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldNotRenderUnusedLeftAndRightHistogramLevels() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 10, 15, 20, 25, 30, 35})
			.build();

		stats.recordValue(12);
		stats.recordValue(14);

		stats.recordValue(23);

		List<String> expected = List.of(
			"[10, 15)  :  2",
			"[15, 20)  :  0",
			"[20, 25)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneIntermediateValue() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 15, 500})
			.build();

		stats.recordValue(17);

		List<String> expected = List.of(
			"[15, 500)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneRightmostValue() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 15, 500})
			.build();

		stats.recordValue(600);

		List<String> expected = List.of(
			"[500, +∞)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneLeftmostValue() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 15, 500})
			.build();

		stats.recordValue(-10);

		List<String> expected = List.of(
			"(-∞,  5)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldAccumulateHistogram() {
		ValueStats stats_1 = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 10, 15})
			.build();
		ValueStats stats_2 = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(new long[]{5, 10, 15})
			.build();

		// first interval
		stats_1.recordValue(2);
		stats_1.recordValue(4);
		stats_2.recordValue(1);

		// second interval
		stats_1.recordValue(8);

		// no data for third interval

		// fourth interval
		stats_2.recordValue(17);

		stats_1.refresh(1L);
		stats_2.refresh(1L);

		ValueStats accumulator = ValueStats.createAccumulator();
		accumulator.add(stats_1);
		accumulator.add(stats_2);

		List<String> expected = List.of(
			"(-∞,  5)  :  3",
			"[ 5, 10)  :  1",
			"[10, 15)  :  0",
			"[15, +∞)  :  1"
		);
		assertEquals(expected, accumulator.getHistogram());
	}

	@Test
	public void itShouldProperlyBuild_Pow2_Histogram() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(POWERS_OF_TWO)
			.build();

		stats.recordValue(-10);

		stats.recordValue(0);
		stats.recordValue(0);

		stats.recordValue(2);
		stats.recordValue(3);
		stats.recordValue(3);

		stats.recordValue(7);

		List<String> expected = List.of(
			"(-∞,  0)  :  1",
			"[ 0,  1)  :  2",
			"[ 1,  2)  :  0",
			"[ 2,  4)  :  3",
			"[ 4,  8)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldProperlyBuild_Pow2_Histogram_withLimitValues() {
		ValueStats stats = ValueStats.builder(SMOOTHING_WINDOW)
			.withHistogram(POWERS_OF_TWO)
			.build();

		stats.recordValue(Long.MAX_VALUE);

		List<String> expected = List.of(
			"[                  0,                   1)  :  0",
			"[                  1,                   2)  :  0",
			"[                  2,                   4)  :  0",
			"[                  4,                   8)  :  0",
			"[                  8,                  16)  :  0",
			"[                 16,                  32)  :  0",
			"[                 32,                  64)  :  0",
			"[                 64,                 128)  :  0",
			"[                128,                 256)  :  0",
			"[                256,                 512)  :  0",
			"[                512,                1024)  :  0",
			"[               1024,                2048)  :  0",
			"[               2048,                4096)  :  0",
			"[               4096,                8192)  :  0",
			"[               8192,               16384)  :  0",
			"[              16384,               32768)  :  0",
			"[              32768,               65536)  :  0",
			"[              65536,              131072)  :  0",
			"[             131072,              262144)  :  0",
			"[             262144,              524288)  :  0",
			"[             524288,             1048576)  :  0",
			"[            1048576,             2097152)  :  0",
			"[            2097152,             4194304)  :  0",
			"[            4194304,             8388608)  :  0",
			"[            8388608,            16777216)  :  0",
			"[           16777216,            33554432)  :  0",
			"[           33554432,            67108864)  :  0",
			"[           67108864,           134217728)  :  0",
			"[          134217728,           268435456)  :  0",
			"[          268435456,           536870912)  :  0",
			"[          536870912,          1073741824)  :  0",
			"[         1073741824,          2147483648)  :  0",
			"[         2147483648,          4294967296)  :  0",
			"[         4294967296,          8589934592)  :  0",
			"[         8589934592,         17179869184)  :  0",
			"[        17179869184,         34359738368)  :  0",
			"[        34359738368,         68719476736)  :  0",
			"[        68719476736,        137438953472)  :  0",
			"[       137438953472,        274877906944)  :  0",
			"[       274877906944,        549755813888)  :  0",
			"[       549755813888,       1099511627776)  :  0",
			"[      1099511627776,       2199023255552)  :  0",
			"[      2199023255552,       4398046511104)  :  0",
			"[      4398046511104,       8796093022208)  :  0",
			"[      8796093022208,      17592186044416)  :  0",
			"[     17592186044416,      35184372088832)  :  0",
			"[     35184372088832,      70368744177664)  :  0",
			"[     70368744177664,     140737488355328)  :  0",
			"[    140737488355328,     281474976710656)  :  0",
			"[    281474976710656,     562949953421312)  :  0",
			"[    562949953421312,    1125899906842624)  :  0",
			"[   1125899906842624,    2251799813685248)  :  0",
			"[   2251799813685248,    4503599627370496)  :  0",
			"[   4503599627370496,    9007199254740992)  :  0",
			"[   9007199254740992,   18014398509481984)  :  0",
			"[  18014398509481984,   36028797018963968)  :  0",
			"[  36028797018963968,   72057594037927936)  :  0",
			"[  72057594037927936,  144115188075855872)  :  0",
			"[ 144115188075855872,  288230376151711744)  :  0",
			"[ 288230376151711744,  576460752303423488)  :  0",
			"[ 576460752303423488, 1152921504606846976)  :  0",
			"[1152921504606846976, 2305843009213693952)  :  0",
			"[2305843009213693952, 4611686018427387904)  :  0",
			"[4611686018427387904,                  +∞)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	public static int uniformRandom(int min, int max) {
		return min + (Math.abs(RANDOM.nextInt(Integer.MAX_VALUE)) % (max - min + 1));
	}

	@Test
	public void itShouldProperlyBuildStringForPositiveNumbers() {
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();
		long inputValue = 1;
		int iterations = 100;
		int rate = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 100;
		}

		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("8518±1408  [7152...9901]  last: 9901  calls: 100 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 1;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("10084.63±67.31  [10059.79...10100]  last: 10100  calls: 200 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 1;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("10186.07±14.58  [10172.11...10200]  last: 10200  calls: 300 @ 1/second", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringForNegativeNumbers() {
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();
		long inputValue = 0;
		int iterations = 100;
		int rate = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 100;
		}

		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-8517±1408  [-9900...-7151]  last: -9900  calls: 100 @ 1/second", valueStats.toString());
		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = 0;
		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 1;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-85.17±14.08  [-99...-71.51]  last: -99  calls: 100 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 1;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-185.07±14.42  [-199...-171.14]  last: -199  calls: 200 @ 1/second", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringForMirroredNumbers() {
		long currentTimestamp = 0;

		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.withRate()
			.build();
		int rate = ONE_SECOND_IN_MILLIS;

		long inputValue = -11L;

		for (int i = 0; i < 12; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 2L;
		}
		assertEquals("1.63±6.79  [-11...11]  last: 11  calls: 12 @ 1.06/second", valueStats.toString());

		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = -101L;
		for (int i = 0; i < 102; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 2;
		}
		assertEquals("73.3±28.2  [-101...101]  last: 101  calls: 102 @ 1/second", valueStats.toString());
	}

	@Test
	public void testFormatterEdgeCases() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();

		// Test if min == max
		long currentTimestamp = 0;
		long inputValue = 1;

		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
		}
		assertEquals("1±0  [1...1]  last: 1  calls: 2 @ 1.933033/second", valueStats.toString());

		// Test if min == 0
		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = 0;
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 1;
		}
		assertEquals("0.5173±0.4997  [0.0346...1]  last: 1  calls: 2 @ 1/second", valueStats.toString());

		// Test if max == 0
		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = -1;
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 1;
		}
		assertEquals("-0.4827±0.4997  [-0.9654...0]  last: 0  calls: 2 @ 1/second", valueStats.toString());
	}

	@Test
	public void testPrecision() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.build();

		long currentTimestamp = 0;
		long inputValue = 0;

		// Test if difference is 1 - precision should be 1/1000 = 0.001 (3 digits)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 1;
		}
		assertNumberOfDigitsAfterDot(valueStats, 3);

		// Test if difference is 10 - precision should be 10/1000 = 0.01 (2 digits)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 10;
		}
		assertNumberOfDigitsAfterDot(valueStats, 2);

		// Test if difference is 100 - precision should be 100/1000 = 0.1 (1 digit)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 100;
		}
		assertNumberOfDigitsAfterDot(valueStats, 1);

		// Test if difference is 1000 - precision should be 1000/1000 = 1 (0 digits)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 1000;
		}
		assertNumberOfDigitsAfterDot(valueStats, 0);

		// Test if precision is 100, difference is 10 - precision should be 10/100 = 0.1 (1 digit)
		inputValue = 0;
		valueStats = ValueStats.builder(SMOOTHING_WINDOW)
			.withPrecision(100)
			.build();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 33;
		}
		assertNumberOfDigitsAfterDot(valueStats, 1);
	}

	@Test
	public void testWithUnit() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withUnit("bytes")
			.build();

		long currentTimestamp = 0;
		long inputValue = 1;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 100;
		}
		assertEquals("8518±1408 bytes  [7152...9901]  last: 9901", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringWithFormatter() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();

		long currentTimestamp = 0;
		long inputValue = 1;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 100;
		}
		assertEquals("8518±1408  [7152...9901]  last: 9901  calls: 100 @ 1/second", valueStats.toString());
	}

	@Test
	public void ifRateIsFloatEventStatsShouldApproximateThatRateAfterEnoughTimePassed() {
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(1));
		long currentTimestamp = 0;
		int events = 10000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < 100; i++) {
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(0, valueStats.getSmoothedRate(), 0.1);

		for (int i = 0; i < events; i++) {
			valueStats.recordValue(RANDOM.nextLong());
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, valueStats.getSmoothedRate(), 1E-5);

		for (int i = 0; i < 250; i++) {
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(0, valueStats.getSmoothedRate(), 0.1);

		for (int i = 0; i < events; i++) {
			valueStats.recordValue(RANDOM.nextLong());
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, valueStats.getSmoothedRate(), 1E-5);
	}

	@Test
	public void itShouldProperlyCalculateRateWithRateUnit() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(1))
			.withRate()
			.build();
		long currentTimestamp = 0;
		int events = 10000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			valueStats.recordValue(RANDOM.nextLong());
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, valueStats.getSmoothedRate(), 0.1);

		assertTrue(valueStats.toString().endsWith("calls: 10000 @ 20/second"));
	}

	@Test
	public void itShouldProperlyCalculateRateWithNamedRateUnit() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(1))
			.withRate("requests")
			.build();
		long currentTimestamp = 0;
		int events = 10000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			valueStats.recordValue(RANDOM.nextLong());
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, valueStats.getSmoothedRate(), 0.1);

		assertTrue(valueStats.toString().endsWith("calls: 10000 @ 20 requests/second"));
	}

	@Test
	public void testScientificNotation() {
		ValueStats valueStats = ValueStats.builder(Duration.ofSeconds(10))
			.withScientificNotation()
			.build();

		long currentTimestamp = 0;
		long inputValue = 123456789;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 123456789;
		}

		assertEquals("1.063766E10±0.0E0  [8.952446E9...1.234568E10]  last: 1.234568E10", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 1;
		}

		assertEquals("1.246735E10±0.0E0  [1.245373E10...1.246914E10]  last: 1.246914E10", valueStats.toString());
	}

	@Test
	public void testValueStatsWithoutComponents() {
		ValueStats valueStats1 = ValueStats.builder(Duration.ofSeconds(10))
			.withAverageAndDeviation(false)
			.build();
		long currentTimestamp = 0;
		long inputValue = 123456789;
		valueStats1.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		assertEquals("[123456789...123456789]  last: 123456789", valueStats1.toString());

		ValueStats valueStats2 = ValueStats.builder(Duration.ofSeconds(10))
			.withMinMax(false)
			.build();
		valueStats2.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		assertEquals("123456789±0  last: 123456789", valueStats2.toString());

		ValueStats valueStats3 = ValueStats.builder(Duration.ofSeconds(10))
			.withLastValue(false)
			.build();
		valueStats3.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		assertEquals("123456789±0  [123456789...123456789]", valueStats3.toString());

		ValueStats valueStats4 = ValueStats.builder(Duration.ofSeconds(10))
			.withLastValue(false)
			.withMinMax(false)
			.withAverageAndDeviation(false)
			.build();
		valueStats4.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats4.refresh(currentTimestamp);
		assertEquals("", valueStats4.toString());
	}

	@Test
	public void testValueStatsWithComponents() {
		ValueStats valueStats1 = ValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();
		long currentTimestamp = 0;
		long inputValue = 123456789;
		valueStats1.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		valueStats1.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		assertEquals("123456789.5173±0  [123456789.0346...123456790]  last: 123456790  calls: 2 @ 1.933/second", valueStats1.toString());

		ValueStats valueStats2 = ValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.build();
		valueStats2.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		valueStats2.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		assertEquals("123456789.517±0  [123456789...123456790]  last: 123456790", valueStats2.toString());

		ValueStats valueStats3 = ValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.withRate()
			.build();
		valueStats3.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		valueStats3.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		assertEquals("123456789.517±0  [123456789...123456790]  last: 123456790  calls: 2 @ 1.933/second", valueStats3.toString());
	}

	private void assertNumberOfDigitsAfterDot(ValueStats stats) {
		String statsString = stats.toString();
		String formattedMin = statsString.substring(statsString.indexOf('[') + 1, statsString.indexOf("..."));
		String formattedMax = statsString.substring(statsString.indexOf("...") + 3, statsString.indexOf("]"));

		int log = (int) floor(log10(stats.getSmoothedMax() - stats.getSmoothedMin()) - 3);
		int numberOfDigits = log > 0 ? 0 : -log;
		numberOfDigits = Math.min(numberOfDigits, 6);
		DecimalFormat format = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.US));
		format.setMaximumFractionDigits(numberOfDigits);

		assertEquals(format.format(stats.getSmoothedMin()), formattedMin);
		assertEquals(format.format(stats.getSmoothedMax()), formattedMax);
	}

	private void assertNumberOfDigitsAfterDot(ValueStats stats, int number) {
		String statsString = stats.toString();
		String formattedAvg = statsString.substring(0, statsString.indexOf("±"));
		String[] splitAvg = formattedAvg.split("\\.");
		if (splitAvg.length == 1) {
			assertEquals(0, number);
			return;
		}
		assertEquals(number, splitAvg[1].length());
	}
}

