package io.activej.csp.process.transformer.impl;

import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.EventloopRule;
import io.activej.test.time.TestCurrentTimeProvider;
import io.activej.test.time.TestCurrentTimeProvider.TimeSequenceCurrentTimeProvider;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RateLimiterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testEmpty() {
		TimeSequenceCurrentTimeProvider timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
			.withTimeProvider(timeSequence)
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();

		RateLimiter<Integer> limiter = RateLimiter.create(100, ChronoUnit.SECONDS);

		List<Integer> expected = IntStream.range(0, 200)
			.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(ChannelSuppliers.ofList(expected)
			.transformWith(limiter)
			.streamTo(ChannelConsumers.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() > 2_000);
	}

	@Test
	public void testHalfFull() {
		TimeSequenceCurrentTimeProvider timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
			.withTimeProvider(timeSequence)
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();

		RateLimiter<Integer> limiter = RateLimiter.<Integer>builder(0.0001, ChronoUnit.MICROS)
			.withInitialTokens(100)
			.build();

		List<Integer> expected = IntStream.range(0, 200)
			.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(ChannelSuppliers.ofList(expected)
			.transformWith(limiter)
			.streamTo(ChannelConsumers.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() > 1_000 && timeSequence.getTime() < 2_000);
	}

	@Test
	public void testFull() {
		TimeSequenceCurrentTimeProvider timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
			.withTimeProvider(timeSequence)
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();

		RateLimiter<Integer> limiter = RateLimiter.<Integer>builder(0.1, ChronoUnit.MILLIS)
			.withInitialTokens(200)
			.build();

		List<Integer> expected = IntStream.range(0, 200)
			.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(ChannelSuppliers.ofList(expected)
			.transformWith(limiter)
			.streamTo(ChannelConsumers.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() < 1_000);
	}
}
