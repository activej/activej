package io.activej.csp.process;

import io.activej.common.exception.FatalErrorHandler;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.EventloopRule;
import io.activej.test.time.TestCurrentTimeProvider;
import io.activej.test.time.TestCurrentTimeProvider.CurrentTimeProvider_TimeSequence;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChannelRateLimiterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testEmpty() {
		CurrentTimeProvider_TimeSequence timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
				.withTimeProvider(timeSequence)
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandler.rethrow())
				.build();

		ChannelRateLimiter<Integer> limiter = ChannelRateLimiter.create(100, ChronoUnit.SECONDS);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(ChannelSupplier.ofList(expected)
				.transformWith(limiter)
				.streamTo(ChannelConsumer.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() > 2_000);
	}

	@Test
	public void testHalfFull() {
		CurrentTimeProvider_TimeSequence timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
				.withTimeProvider(timeSequence)
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandler.rethrow())
				.build();

		ChannelRateLimiter<Integer> limiter = ChannelRateLimiter.<Integer>builder(0.0001, ChronoUnit.MICROS)
				.withInitialTokens(100)
				.build();

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(ChannelSupplier.ofList(expected)
				.transformWith(limiter)
				.streamTo(ChannelConsumer.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() > 1_000 && timeSequence.getTime() < 2_000);
	}

	@Test
	public void testFull() {
		CurrentTimeProvider_TimeSequence timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
				.withTimeProvider(timeSequence)
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandler.rethrow())
				.build();

		ChannelRateLimiter<Integer> limiter = ChannelRateLimiter.<Integer>builder(0.1, ChronoUnit.MILLIS)
				.withInitialTokens(200)
				.build();

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(ChannelSupplier.ofList(expected)
				.transformWith(limiter)
				.streamTo(ChannelConsumer.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() < 1_000);
	}
}
