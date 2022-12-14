package io.activej.datastream.processor;

import io.activej.common.ref.RefLong;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.EventloopRule;
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
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		StreamRateLimiter<Integer> limiter = StreamRateLimiter.create(100, ChronoUnit.SECONDS);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		RefLong passed = new RefLong(eventloop.currentTimeMillis());
		await(StreamSupplier.ofIterable(expected)
				.transformWith(limiter)
				.streamTo(StreamConsumer.ofConsumer(actual::add))
				.whenResult(() -> passed.value = eventloop.currentTimeMillis() - passed.value));

		assertEquals(expected, actual);
		assertTrue(passed.value > 2_000);
	}

	@Test
	public void testHalfFull() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		StreamRateLimiter<Integer> limiter = StreamRateLimiter.<Integer>create(0.1, ChronoUnit.MILLIS)
				.withInitialTokens(100L);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		RefLong passed = new RefLong(eventloop.currentTimeMillis());
		await(StreamSupplier.ofIterable(expected)
				.transformWith(limiter)
				.streamTo(StreamConsumer.ofConsumer(actual::add))
				.whenResult(() -> passed.value = eventloop.currentTimeMillis() - passed.value));

		assertEquals(expected, actual);
		assertTrue(passed.value > 1_000 && passed.value < 2_000);
	}

	@Test
	public void testFull() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		StreamRateLimiter<Integer> limiter = StreamRateLimiter.<Integer>create(0.0001, ChronoUnit.MICROS)
				.withInitialTokens(200L);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		RefLong passed = new RefLong(eventloop.currentTimeMillis());
		await(StreamSupplier.ofIterable(expected)
				.transformWith(limiter)
				.streamTo(StreamConsumer.ofConsumer(actual::add))
				.whenResult(() -> passed.value = eventloop.currentTimeMillis() - passed.value));

		assertEquals(expected, actual);
		assertTrue(passed.value < 1_000);
	}

}
