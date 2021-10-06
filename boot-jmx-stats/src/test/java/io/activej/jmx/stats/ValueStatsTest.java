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
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
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
	public void itShouldBuildHistogram() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

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

		List<String> expected = asList(
				"( -∞,   5)  :  14",
				"[  5,  15)  :   3",
				"[ 15, 500)  :   0",
				"[500,  +∞)  :   2"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldNotRenderUnusedLeftAndRightHistogramLevels() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 10, 15, 20, 25, 30, 35});

		stats.recordValue(12);
		stats.recordValue(14);

		stats.recordValue(23);

		List<String> expected = asList(
				"[10, 15)  :  2",
				"[15, 20)  :  0",
				"[20, 25)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneIntermediateValue() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		stats.recordValue(17);

		List<String> expected = asList(
				"[15, 500)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneRightmostValue() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		stats.recordValue(600);

		List<String> expected = asList(
				"[500, +∞)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldBuildHistogramProperlyInCaseOfOnlyOneLeftmostValue() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 15, 500});

		stats.recordValue(-10);

		List<String> expected = asList(
				"(-∞,  5)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	@Test
	public void itShouldAccumulateHistogram() {
		ValueStats stats_1 = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 10, 15});
		ValueStats stats_2 = ValueStats.create(SMOOTHING_WINDOW).withHistogram(new int[]{5, 10, 15});

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

		List<String> expected = asList(
				"(-∞,  5)  :  3",
				"[ 5, 10)  :  1",
				"[10, 15)  :  0",
				"[15, +∞)  :  1"
		);
		assertEquals(expected, accumulator.getHistogram());
	}

	@Test
	public void itShouldProperlyBuild_Pow2_Histogram() {
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);

		stats.recordValue(-10);

		stats.recordValue(0);
		stats.recordValue(0);

		stats.recordValue(2);
		stats.recordValue(3);
		stats.recordValue(3);

		stats.recordValue(7);

		List<String> expected = asList(
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
		ValueStats stats = ValueStats.create(SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);

		stats.recordValue(Integer.MAX_VALUE);

		List<String> expected = asList(
				"[         0,          1)  :  0",
				"[         1,          2)  :  0",
				"[         2,          4)  :  0",
				"[         4,          8)  :  0",
				"[         8,         16)  :  0",
				"[        16,         32)  :  0",
				"[        32,         64)  :  0",
				"[        64,        128)  :  0",
				"[       128,        256)  :  0",
				"[       256,        512)  :  0",
				"[       512,       1024)  :  0",
				"[      1024,       2048)  :  0",
				"[      2048,       4096)  :  0",
				"[      4096,       8192)  :  0",
				"[      8192,      16384)  :  0",
				"[     16384,      32768)  :  0",
				"[     32768,      65536)  :  0",
				"[     65536,     131072)  :  0",
				"[    131072,     262144)  :  0",
				"[    262144,     524288)  :  0",
				"[    524288,    1048576)  :  0",
				"[   1048576,    2097152)  :  0",
				"[   2097152,    4194304)  :  0",
				"[   4194304,    8388608)  :  0",
				"[   8388608,   16777216)  :  0",
				"[  16777216,   33554432)  :  0",
				"[  33554432,   67108864)  :  0",
				"[  67108864,  134217728)  :  0",
				"[ 134217728,  268435456)  :  0",
				"[ 268435456,  536870912)  :  0",
				"[ 536870912, 1073741824)  :  0",
				"[1073741824,         +∞)  :  1"
		);
		assertEquals(expected, stats.getHistogram());
	}

	public static int uniformRandom(int min, int max) {
		return min + (Math.abs(RANDOM.nextInt(Integer.MAX_VALUE)) % (max - min + 1));
	}

	@Test
	public void itShouldProperlyBuildStringForPositiveNumbers() {
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withRate();
		double inputValue = 1;
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

			inputValue += 0.01;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("10000.4±63.89  [9989.24...10001.99]  last: 10001.99  calls: 200 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 0.00001;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("10001.99929±1.99715  [10001.97781...10002.00099]  last: 10002.00099  calls: 300 @ 1/second", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringForNegativeNumbers() {
		long currentTimestamp = 0;
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withRate();
		double inputValue = 0;
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

			inputValue -= 0.01;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-0.8517±0.1408  [-0.99...-0.7151]  last: -0.99  calls: 100 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 0.00001;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-1.000706±0.006408  [-1.00099...-0.999465]  last: -1.00099  calls: 200 @ 1/second", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringForMirroredNumbers() {
		long currentTimestamp = 0;

		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withAbsoluteValues(true).withRate();
		int rate = ONE_SECOND_IN_MILLIS;

		double inputValue = -1.1;

		for (int i = 0; i < 11; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 0.22;
		}
		assertEquals("0.151±0.686  [-1.1...1.1]  last: 1.1  calls: 11 @ 1.067/second", valueStats.toString());

		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = -10.1;
		for (int i = 0; i < 101; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 0.202;
		}
		assertEquals("7.3±2.85  [-10.1...10.1]  last: 10.1  calls: 101 @ 1/second", valueStats.toString());
	}

	@Test
	public void testFormatterEdgeCases() {
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withRate();

		// Test if min == max
		long currentTimestamp = 0;
		double inputValue = 0.001;

		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
		}
		assertEquals("0.001±0  [0.001...0.001]  last: 0.001  calls: 2 @ 1.933033/second", valueStats.toString());

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
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withAbsoluteValues(true);

		long currentTimestamp = 0;
		double inputValue = 0.123456789123456789d;

		// Test if difference is 0.1 - precision should be 0.1/1000 = 0.0001 (4 digits)
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 0.1;
		}
		assertNumberOfDigitsAfterDot(valueStats, 4);

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
		valueStats.resetStats();
		inputValue = 0.123456789;
		valueStats.withPrecision(100);
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 33.3;
		}
		assertNumberOfDigitsAfterDot(valueStats, 1);
	}

	@Test
	public void testWithUnit() {
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withUnit("bytes");

		long currentTimestamp = 0;
		double inputValue = 1;
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
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withRate();

		long currentTimestamp = 0;
		double inputValue = 1;
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
			valueStats.recordValue(RANDOM.nextDouble());
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
			valueStats.recordValue(RANDOM.nextDouble());
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, valueStats.getSmoothedRate(), 1E-5);
	}

	@Test
	public void itShouldProperlyCalculateRateWithRateUnit() {
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(1)).withRate();
		long currentTimestamp = 0;
		int events = 10000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			valueStats.recordValue(RANDOM.nextDouble());
			valueStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, valueStats.getSmoothedRate(), 0.1);

		assertTrue(valueStats.toString().endsWith("calls: 10000 @ 20/second"));

		valueStats.withRate("messages");
		assertTrue(valueStats.toString().endsWith("calls: 10000 @ 20 messages/second"));

		valueStats.withRate("requests");
		assertTrue(valueStats.toString().endsWith("calls: 10000 @ 20 requests/second"));
	}

	@Test
	public void testScientificNotation() {
		ValueStats valueStats = ValueStats.create(Duration.ofSeconds(10)).withScientificNotation();

		long currentTimestamp = 0;
		double inputValue = 1.23456789;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 1.23456789;
		}

		assertEquals("1.063766E2±1.73837E1  [8.952446E1...1.234568E2]  last: 1.234568E2", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 0.0000000000000001;
		}

		assertEquals("1.246735E2±7.885133E-1  [1.245373E2...1.246902E2]  last: 1.246914E2", valueStats.toString());
	}

	@Test
	public void testValueStatsWithoutComponents() {
		ValueStats valueStats1 = ValueStats.create(Duration.ofSeconds(10)).withAverageAndDeviation(false);
		long currentTimestamp = 0;
		double inputValue = 1.23456789;
		valueStats1.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		assertEquals("[1.234568...1.234568]  last: 1.234568", valueStats1.toString());

		ValueStats valueStats2 = ValueStats.create(Duration.ofSeconds(10)).withMinMax(false);
		valueStats2.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		assertEquals("1.234568±0  last: 1.234568", valueStats2.toString());

		ValueStats valueStats3 = ValueStats.create(Duration.ofSeconds(10)).withLastValue(false);
		valueStats3.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		assertEquals("1.234568±0  [1.234568...1.234568]", valueStats3.toString());

		ValueStats valueStats4 = ValueStats.create(Duration.ofSeconds(10)).withLastValue(false).withMinMax(false).withAverageAndDeviation(false);
		valueStats4.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats4.refresh(currentTimestamp);
		assertEquals("", valueStats4.toString());
	}

	@Test
	public void testValueStatsWithComponents() {
		ValueStats valueStats1 = ValueStats.create(Duration.ofSeconds(10)).withRate();
		long currentTimestamp = 0;
		double inputValue = 1.23456789;
		valueStats1.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		valueStats1.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		assertEquals("1.7519±0.4997  [1.2692...2.2346]  last: 2.2346  calls: 2 @ 1.933/second", valueStats1.toString());

		ValueStats valueStats2 = ValueStats.create(Duration.ofSeconds(10)).withAbsoluteValues(true);
		valueStats2.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		valueStats2.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		assertEquals("1.752±0.5  [1.235...2.235]  last: 2.235", valueStats2.toString());

		ValueStats valueStats3 = ValueStats.create(Duration.ofSeconds(10)).withAbsoluteValues(true).withRate();
		valueStats3.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		valueStats3.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		assertEquals("1.752±0.5  [1.235...2.235]  last: 2.235  calls: 2 @ 1.933/second", valueStats3.toString());
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
		String formattedMin = statsString.substring(statsString.indexOf('[') + 1, statsString.indexOf("..."));
		String formattedMax = statsString.substring(statsString.indexOf("...") + 3, statsString.indexOf("]"));
		String[] splitMin = formattedMin.split("\\.");
		String[] splitMax = formattedMax.split("\\.");
		if (splitMin.length == 1 && splitMax.length == 1) {
			assertEquals(0, number);
			return;
		}
		assertEquals(number, splitMin[1].length());
		assertEquals(number, splitMax[1].length());
	}
}

