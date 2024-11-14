package io.activej.jmx.stats;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EventStatsTest {

	private static final Random RANDOM = new Random();

	@Test
	public void ifRateIsConstantEventStatsShouldApproximateThatRateAfterEnoughTimePassed() {
		Duration smoothingWindow = Duration.ofSeconds(1);
		EventStats eventStats = EventStats.create(smoothingWindow);
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}

		double acceptableError = 1E-5;
		assertEquals(rate, eventStats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void ifRateIsFloatEventStatsShouldApproximateThatRateAfterEnoughTimePassed() {
		EventStats eventStats = EventStats.create(Duration.ofSeconds(1));
		long currentTimestamp = 0;
		int events = 10000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < 100; i++) {
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(0, eventStats.getSmoothedRate(), 0.1);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, eventStats.getSmoothedRate(), 1E-5);

		for (int i = 0; i < 250; i++) {
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(0, eventStats.getSmoothedRate(), 0.1);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals(rate, eventStats.getSmoothedRate(), 1E-5);
	}

	@Test
	public void counterShouldResetRateAfterResetMethodCall() {
		Duration smoothingWindow = Duration.ofSeconds(1);
		EventStats eventStats = EventStats.create(smoothingWindow);
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			currentTimestamp += periodInMillis;
			eventStats.refresh(currentTimestamp);
		}
		double rateBeforeReset = eventStats.getSmoothedRate();
		eventStats.resetStats();
		double rateAfterReset = eventStats.getSmoothedRate();

		double initRateOfReset = 0.0;
		double acceptableError = 1E-5;
		assertEquals(rate, rateBeforeReset, acceptableError);
		assertEquals(initRateOfReset, rateAfterReset, acceptableError);
	}

	@Test
	public void counterShouldProperlyAggregateEvents() {
		int iterations = 1000;
		int period = 100;
		long currentTimestamp = 0;

		Duration smoothingWindow = Duration.ofSeconds(10);

		EventStats stats_1 = EventStats.create(smoothingWindow);
		EventStats stats_2 = EventStats.create(smoothingWindow);
		EventStats stats_3 = EventStats.create(smoothingWindow);

		for (int i = 0; i < iterations; i++) {
			stats_1.recordEvents(1);
			stats_2.recordEvents(2);
			// we do not record event to stats_3

			currentTimestamp += period;
			stats_1.refresh(currentTimestamp);
			stats_2.refresh(currentTimestamp);
			stats_3.refresh(currentTimestamp);
		}

		EventStats accumulator = EventStats.createAccumulator();
		accumulator.add(stats_1);
		accumulator.add(stats_2);
		accumulator.add(stats_3);

		double acceptableError = 0.1;
		assertEquals(30.0, accumulator.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldProperlyAggregateEventsWithOriginalRateUnits() {
		int iterations = 1000;
		int period = 100;
		long currentTimestamp = 0;

		Duration smoothingWindow = Duration.ofSeconds(10);

		EventStats stats_1 = EventStats.builder(smoothingWindow).withRateUnit("requests").build();
		EventStats stats_2 = EventStats.builder(smoothingWindow).withRateUnit("requests").build();
		EventStats stats_3 = EventStats.builder(smoothingWindow).withRateUnit("requests").build();

		for (int i = 0; i < iterations; i++) {
			stats_1.recordEvents(1);
			stats_2.recordEvents(2);
			// we do not record event to stats_3

			currentTimestamp += period;
			stats_1.refresh(currentTimestamp);
			stats_2.refresh(currentTimestamp);
			stats_3.refresh(currentTimestamp);
		}

		EventStats accumulator = EventStats.createAccumulator();
		accumulator.add(stats_1);
		accumulator.add(stats_2);
		accumulator.add(stats_3);

		MatcherAssert.assertThat(accumulator.toString(), containsString("requests"));
	}

	@Test
	public void itShouldConvergeProperlyWhenNoEventsOccurredBetweenRefreshes() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		EventStats stats = EventStats.create(smoothingWindow);
		int iterations = 1000;
		long currentTimestamp = 0;
		int period = 200;
		for (int i = 0; i < iterations; i++) {
			// event occurs only once per two refreshes
			int eventsPerRefresh = i % 2;
			stats.recordEvents(eventsPerRefresh);
			currentTimestamp += period;
			stats.refresh(currentTimestamp);
		}

		assertEquals(500, stats.getTotalCount());
		double acceptableError = 0.25;
		assertEquals(2.5, stats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenPeriodIsNotStableButPeriodExpectationIsStable() {
		Duration smoothingWindow = Duration.ofSeconds(10);
		EventStats stats = EventStats.create(smoothingWindow);
		int iterations = 1000;
		long currentTimestamp = 0;
		int period = 200;
		for (int i = 0; i < iterations; i++) {
			stats.recordEvents(1);
			int currentPeriod = period + uniformRandom(-50, 50);
			currentTimestamp += currentPeriod;
			stats.refresh(currentTimestamp);
		}

		assertEquals(1000, stats.getTotalCount());
		double acceptableError = 0.25;
		assertEquals(5.0, stats.getSmoothedRate(), acceptableError);
	}

	@Test
	public void itShouldConvergeProperlyWhenAmountOfEventsPerPeriodIsNotStableButExpectationOfAmountOfEventIsStable() {
		Duration smoothingWindow = Duration.ofSeconds(50);
		EventStats stats = EventStats.create(smoothingWindow);
		int iterations = 10000;
		long currentTimestamp = 0;
		int period = 200;
		for (int i = 0; i < iterations; i++) {
			stats.recordEvents(uniformRandom(0, 10));
			currentTimestamp += period;
			stats.refresh(currentTimestamp);
		}

		double acceptableError = 2.0;
		assertEquals(25.0, stats.getSmoothedRate(), acceptableError);
	}

	public static int uniformRandom(int min, int max) {
		return min + Math.abs(RANDOM.nextInt(Integer.MAX_VALUE)) % (max - min + 1);
	}

	@Test
	public void itShouldProperlyBuildString() {
		Duration smoothingWindow = Duration.ofSeconds(1);
		EventStats eventStats = EventStats.create(smoothingWindow);
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 6;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals("1000 @ 6.024/second", eventStats.toString());

		rate = 0.0555464;
		period = 1.0 / rate;
		periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events * 5; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}

		assertEquals("6000 @ 0.05555/second", eventStats.toString());
	}

	@Test
	public void itShouldProperlyBuildStringWithRateUnits() {
		Duration smoothingWindow = Duration.ofSeconds(1);
		EventStats eventStats = EventStats.builder(smoothingWindow)
			.withRateUnit("records")
			.build();
		long currentTimestamp = 0;
		int events = 1000;
		double rate = 6;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertEquals("1000 @ 6.024 records/second", eventStats.toString());

		rate = 0.0555464;
		period = 1.0 / rate;
		periodInMillis = (int) (period * 1000);

		for (int i = 0; i < events * 5; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}

		assertEquals("6000 @ 0.05555 records/second", eventStats.toString());
	}

	@Test
	public void testPrecision() {
		Duration smoothingWindow = Duration.ofSeconds(1);
		EventStats eventStats = EventStats.create(smoothingWindow);
		long currentTimestamp = 0;
		int events = 1000;

		// Rate is ~ 0.1 - precision should be 0.1/1000 = 0.0001 (4 digits)
		double rate = 0.11234567;
		double period = 1.0 / rate;
		int periodInMillis = (int) (period * 1000);
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertNumberOfDigitsAfterDot(eventStats, 4);

		// Rate is ~ 1 - precision should be 1/1000 = 0.001 (3 digits)
		eventStats.resetStats();
		rate = 1.1234567;
		period = 1.0 / rate;
		periodInMillis = (int) (period * 1000);
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertNumberOfDigitsAfterDot(eventStats, 3);

		// Rate is ~ 10 - precision should be 10/1000 = 0.01 (2 digits)
		eventStats.resetStats();
		rate = 10.2345678;
		period = 1.0 / rate;
		periodInMillis = (int) (period * 1000);
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertNumberOfDigitsAfterDot(eventStats, 2);

		// Rate is ~ 100 - precision should be 100/1000 = 0.1 (1 digit)
		eventStats.resetStats();
		rate = 100.12345678;
		period = 1.0 / rate;
		periodInMillis = (int) (period * 1000);
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertNumberOfDigitsAfterDot(eventStats, 1);

		// Rate is ~ 1000 - precision should be 1000/1000 = 1 (0 digits)
		eventStats.resetStats();
		rate = 1000.12345678;
		period = 1.0 / rate;
		periodInMillis = (int) (period * 1000);
		for (int i = 0; i < events; i++) {
			eventStats.recordEvent();
			eventStats.refresh(currentTimestamp);
			currentTimestamp += periodInMillis;
		}
		assertNumberOfDigitsAfterDot(eventStats, 0);
	}

	@Test
	public void itShouldNotPrintNaNRate() {
		Duration smoothingWindow = Duration.ofSeconds(1);
		EventStats eventStats = EventStats.create(smoothingWindow);

		eventStats.recordEvent();
		eventStats.refresh(System.currentTimeMillis());

		assertFalse(eventStats.toString().contains("NaN"));
	}

	private void assertNumberOfDigitsAfterDot(EventStats stats, int number) {
		String statsString = stats.toString();
		assert statsString != null;
		String formattedRate = statsString.substring(statsString.indexOf('@') + 2, statsString.indexOf("/"));
		String[] splitRate = formattedRate.split("\\.");
		if (splitRate.length == 1) {
			assertEquals(0, number);
			return;
		}
		assertEquals(number, splitRate[1].length());
	}
}
