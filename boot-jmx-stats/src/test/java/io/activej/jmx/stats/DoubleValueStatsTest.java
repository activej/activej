package io.activej.jmx.stats;

import org.junit.Test;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.util.Locale;
import java.util.Random;

import static java.lang.Math.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DoubleValueStatsTest {
	private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private static final int ONE_SECOND_IN_MILLIS = 1000;
	private static final Random RANDOM = new Random();

	@Test
	public void smoothedAverageAtLimitShouldBeSameAsInputInCaseOfConstantData() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		long currentTimestamp = 0;
		DoubleValueStats valueStats = DoubleValueStats.create(smoothingWindow);
		double inputValue = 5.1;
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
		DoubleValueStats valueStats = DoubleValueStats.create(Duration.ofSeconds(10));
		double inputValue = 5.0001;
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
		DoubleValueStats valueStats = DoubleValueStats.create(smoothingWindow);
		int iterations = 10000;
		double minValue = 0.000001;
		double maxValue = 9.999999;
		int refreshPeriod = 100;

		for (int i = 0; i < iterations; i++) {
			double currentValue = uniformRandom(minValue, maxValue);
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
		DoubleValueStats valueStats = DoubleValueStats.create(smoothingWindow);
		double inputValue = 5.00001;
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
		DoubleValueStats valueStats_1 = DoubleValueStats.create(smoothingWindow);
		DoubleValueStats valueStats_2 = DoubleValueStats.create(smoothingWindow);
		double inputValue_1 = 5.0000001;
		double inputValue_2 = 10.0000001;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			valueStats_1.recordValue(inputValue_1);
			valueStats_2.recordValue(inputValue_2);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats_1.refresh(currentTimestamp);
			valueStats_2.refresh(currentTimestamp);
		}

		DoubleValueStats accumulator = DoubleValueStats.createAccumulator();
		accumulator.add(valueStats_1);
		accumulator.add(valueStats_2);

		double acceptableError = 10E-5;
		double expectedAccumulatedSmoothedAvg = (inputValue_1 + inputValue_2) / 2.0;
		assertEquals(expectedAccumulatedSmoothedAvg, accumulator.getSmoothedAverage(), acceptableError);
	}

	@Test
	public void itShouldAccumulateWithOriginalUnits() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		long currentTimestamp = 0;
		DoubleValueStats valueStats_1 = DoubleValueStats.builder(smoothingWindow).withUnit("seconds").build();
		DoubleValueStats valueStats_2 = DoubleValueStats.builder(smoothingWindow).withUnit("seconds").build();
		double inputValue_1 = 5.00000001;
		double inputValue_2 = 10.00000001;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			valueStats_1.recordValue(inputValue_1);
			valueStats_2.recordValue(inputValue_2);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats_1.refresh(currentTimestamp);
			valueStats_2.refresh(currentTimestamp);
		}

		DoubleValueStats accumulator = DoubleValueStats.createAccumulator();
		accumulator.add(valueStats_1);
		accumulator.add(valueStats_2);

		assertThat(accumulator.toString(), containsString("seconds"));
	}

	public static double uniformRandom(double min, double max) {
		return min + (Math.abs(RANDOM.nextInt(Integer.MAX_VALUE)) % (max - min + 1));
	}

	@Test
	public void itShouldProperlyBuildStringForPositiveNumbers() {
		long currentTimestamp = 0;
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();
		double inputValue = 1.02345;
		int iterations = 100;
		int rate = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 100.0456789;
		}

		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("8521±1409  [7156...9906]  last: 9906  calls: 100 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 1.04321;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("10092.9±67.55  [10067.45...10108.87]  last: 10108.87  calls: 200 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 1.05612;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("10199.75±15.38  [10185.02...10214.47]  last: 10214.47  calls: 300 @ 1/second", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringForNegativeNumbers() {
		long currentTimestamp = 0;
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();
		double inputValue = -0.12345678;
		int iterations = 100;
		int rate = ONE_SECOND_IN_MILLIS;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 100.1234567;
		}

		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-8527±1410  [-9912...-7160]  last: -9912  calls: 100 @ 1/second", valueStats.toString());
		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = 0.0123;
		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 1.05212;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-89.59±14.81  [-104.15...-75.23]  last: -104.15  calls: 100 @ 1/second", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue -= 1.05231;
		}
		assertNumberOfDigitsAfterDot(valueStats);
		assertEquals("-194.72±15.18  [-209.38...-180.06]  last: -209.38  calls: 200 @ 1/second", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringForMirroredNumbers() {
		long currentTimestamp = 0;

		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.withRate()
			.build();
		int rate = ONE_SECOND_IN_MILLIS;

		double inputValue = -11.111111;

		for (int i = 0; i < 12; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 2.02;
		}
		assertEquals("1.65±6.85  [-11.11...11.11]  last: 11.11  calls: 12 @ 1.06/second", valueStats.toString());

		valueStats.resetStats();
		valueStats.refresh(currentTimestamp);
		inputValue = -101.11111;
		for (int i = 0; i < 102; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += rate;
			valueStats.refresh(currentTimestamp);

			inputValue += 2.002;
		}
		assertEquals("73.4±28.3  [-101.1...101.1]  last: 101.1  calls: 102 @ 1/second", valueStats.toString());
	}

	@Test
	public void testFormatterEdgeCases() {
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();

		// Test if min == max
		long currentTimestamp = 0;
		double inputValue = 1.234;

		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
		}
		assertEquals("1.234±0  [1.234...1.234]  last: 1.234  calls: 2 @ 1.933033/second", valueStats.toString());

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
		inputValue = -1.8;
		for (int i = 0; i < 3; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 0.9;
		}
		assertEquals("-0.858±0.734  [-1.708...0]  last: 0  calls: 3 @ 1/second", valueStats.toString());
	}

	@Test
	public void testPrecision() {
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.build();

		long currentTimestamp = 0;
		double inputValue = 0;

		// Test if difference is 1 - precision should be 1/1000 = 0.001 (3 digits)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 1.01010101;
		}
		assertNumberOfDigitsAfterDot(valueStats, 3);

		// Test if difference is 10 - precision should be 10/1000 = 0.01 (2 digits)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 10.123456;
		}
		assertNumberOfDigitsAfterDot(valueStats, 2);

		// Test if difference is 100 - precision should be 100/1000 = 0.1 (1 digit)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 100.123456;
		}
		assertNumberOfDigitsAfterDot(valueStats, 1);

		// Test if difference is 1000 - precision should be 1000/1000 = 1 (0 digits)
		valueStats.resetStats();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 1000.01010101;
		}
		assertNumberOfDigitsAfterDot(valueStats, 0);

		// Test if precision is 100, difference is 10 - precision should be 10/100 = 0.1 (1 digit)
		inputValue = 0;
		valueStats = DoubleValueStats.builder(SMOOTHING_WINDOW)
			.withPrecision(100)
			.build();
		for (int i = 0; i < 2; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);
			inputValue += 33.03030303;
		}
		assertNumberOfDigitsAfterDot(valueStats, 1);
	}

	@Test
	public void testWithUnit() {
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withUnit("bytes")
			.build();

		long currentTimestamp = 0;
		double inputValue = 1.01234;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 1.12345;
		}
		assertEquals("96.69±15.82 bytes  [81.36...112.23]  last: 112.23", valueStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringWithFormatter() {
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();

		long currentTimestamp = 0;
		double inputValue = 1.01234;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 1.01234;
		}
		assertEquals("87.23±14.25  [73.41...101.23]  last: 101.23  calls: 100 @ 1/second", valueStats.toString());
	}

	@Test
	public void ifRateIsFloatEventStatsShouldApproximateThatRateAfterEnoughTimePassed() {
		DoubleValueStats valueStats = DoubleValueStats.create(Duration.ofSeconds(1));
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
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(1))
			.withRate()
			.build();
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
	}

	@Test
	public void itShouldProperlyCalculateRateWithNamedRateUnit() {
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(1))
			.withRate("requests")
			.build();
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

		assertTrue(valueStats.toString().endsWith("calls: 10000 @ 20 requests/second"));
	}

	@Test
	public void testScientificNotation() {
		DoubleValueStats valueStats = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withScientificNotation()
			.build();

		long currentTimestamp = 0;
		double inputValue = 123456789.123;
		int iterations = 100;

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 123456789.123;
		}

		assertEquals("1.063766E10±1.73837E9  [8.952446E9...1.234568E10]  last: 1.234568E10", valueStats.toString());

		for (int i = 0; i < iterations; i++) {
			valueStats.recordValue(inputValue);
			currentTimestamp += ONE_SECOND_IN_MILLIS;
			valueStats.refresh(currentTimestamp);

			inputValue += 1.010101;
		}

		assertEquals("1.246735E10±7.885133E7  [1.245373E10...1.246914E10]  last: 1.246914E10", valueStats.toString());
	}

	@Test
	public void testValueStatsWithoutComponents() {
		DoubleValueStats valueStats1 = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withAverageAndDeviation(false)
			.build();
		long currentTimestamp = 0;
		double inputValue = 123456789.1234;
		valueStats1.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		assertEquals("[123456789.1234...123456789.1234]  last: 123456789.1234", valueStats1.toString());

		DoubleValueStats valueStats2 = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withMinMax(false)
			.build();
		valueStats2.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		assertEquals("123456789.1234±0  last: 123456789.1234", valueStats2.toString());

		DoubleValueStats valueStats3 = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withLastValue(false)
			.build();
		valueStats3.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		assertEquals("123456789.1234±0  [123456789.1234...123456789.1234]", valueStats3.toString());

		DoubleValueStats valueStats4 = DoubleValueStats.builder(Duration.ofSeconds(10))
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
		DoubleValueStats valueStats1 = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withRate()
			.build();
		long currentTimestamp = 0;
		double inputValue = 123456789.123;
		valueStats1.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		valueStats1.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats1.refresh(currentTimestamp);
		assertEquals("123456789.6403±0  [123456789.1576...123456790.123]  last: 123456790.123  calls: 2 @ 1.933/second", valueStats1.toString());

		DoubleValueStats valueStats2 = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.build();
		valueStats2.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		valueStats2.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats2.refresh(currentTimestamp);
		assertEquals("123456789.64±0  [123456789.123...123456790.123]  last: 123456790.123", valueStats2.toString());

		DoubleValueStats valueStats3 = DoubleValueStats.builder(Duration.ofSeconds(10))
			.withAbsoluteValues(true)
			.withRate()
			.build();
		valueStats3.recordValue(inputValue);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		valueStats3.recordValue(inputValue + 1);
		currentTimestamp += ONE_SECOND_IN_MILLIS;
		valueStats3.refresh(currentTimestamp);
		assertEquals("123456789.64±0  [123456789.123...123456790.123]  last: 123456790.123  calls: 2 @ 1.933/second", valueStats3.toString());
	}

	private void assertNumberOfDigitsAfterDot(DoubleValueStats stats) {
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

	private void assertNumberOfDigitsAfterDot(DoubleValueStats stats, int number) {
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

