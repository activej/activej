package io.activej.datastream.processor;

import io.activej.common.exception.FatalErrorHandler;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.EventloopRule;
import io.activej.test.time.TestCurrentTimeProvider;
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

public class StreamRateLimiterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testEmpty() {
		TestCurrentTimeProvider.TimeSequenceCurrentTimeProvider timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
				.withTimeProvider(timeSequence)
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandler.rethrow())
				.build();

		StreamRateLimiter<Integer> limiter = StreamRateLimiter.create(100, ChronoUnit.SECONDS);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(StreamSupplier.ofIterable(expected)
				.transformWith(limiter)
				.streamTo(StreamConsumer.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() > 2_000);
	}

	@Test
	public void testHalfFull() {
		TestCurrentTimeProvider.TimeSequenceCurrentTimeProvider timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
				.withTimeProvider(timeSequence)
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandler.rethrow())
				.build();

		StreamRateLimiter<Integer> limiter = StreamRateLimiter.<Integer>builder(0.1, ChronoUnit.MILLIS)
				.withInitialTokens(100L)
				.build();

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(StreamSupplier.ofIterable(expected)
				.transformWith(limiter)
				.streamTo(StreamConsumer.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() > 1_000 && timeSequence.getTime() < 2_000);
	}

	@Test
	public void testFull() {
		TestCurrentTimeProvider.TimeSequenceCurrentTimeProvider timeSequence = TestCurrentTimeProvider.ofTimeSequence(0, 10);
		Eventloop.builder()
				.withTimeProvider(timeSequence)
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandler.rethrow())
				.build();

		StreamRateLimiter<Integer> limiter = StreamRateLimiter.<Integer>builder(0.0001, ChronoUnit.MICROS)
				.withInitialTokens(200L)
				.build();

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		await(StreamSupplier.ofIterable(expected)
				.transformWith(limiter)
				.streamTo(StreamConsumer.ofConsumer(actual::add)));

		assertEquals(expected, actual);
		assertTrue(timeSequence.getTime() < 1_000);
	}

}
