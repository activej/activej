package io.activej.csp.process;

import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.reactor.Reactor;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChannelRateLimiterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testEmpty() {
		Reactor reactor = getCurrentReactor();
		ChannelRateLimiter<Integer> limiter = ChannelRateLimiter.create(100, ChronoUnit.SECONDS);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		RefLong passed = new RefLong(reactor.currentTimeMillis());
		await(ChannelSupplier.ofList(expected)
				.transformWith(limiter)
				.streamTo(ChannelConsumer.ofConsumer(actual::add))
				.whenResult(() -> passed.value = reactor.currentTimeMillis() - passed.value));

		assertEquals(expected, actual);
		assertTrue(passed.value > 2_000);
	}

	@Test
	public void testHalfFull() {
		Reactor reactor = getCurrentReactor();
		ChannelRateLimiter<Integer> limiter = ChannelRateLimiter.<Integer>create(0.0001, ChronoUnit.MICROS)
				.withInitialTokens(100);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		RefLong passed = new RefLong(reactor.currentTimeMillis());
		await(ChannelSupplier.ofList(expected)
				.transformWith(limiter)
				.streamTo(ChannelConsumer.ofConsumer(actual::add))
				.whenResult(() -> passed.value = reactor.currentTimeMillis() - passed.value));

		assertEquals(expected, actual);
		assertTrue(passed.value > 1_000 && passed.value < 2_000);
	}

	@Test
	public void testFull() {
		Reactor reactor = getCurrentReactor();
		ChannelRateLimiter<Integer> limiter = ChannelRateLimiter.<Integer>create(0.1, ChronoUnit.MILLIS)
				.withInitialTokens(200);

		List<Integer> expected = IntStream.range(0, 200)
				.boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();

		RefLong passed = new RefLong(reactor.currentTimeMillis());
		await(ChannelSupplier.ofList(expected)
				.transformWith(limiter)
				.streamTo(ChannelConsumer.ofConsumer(actual::add))
				.whenResult(() -> passed.value = reactor.currentTimeMillis() - passed.value));

		assertEquals(expected, actual);
		assertTrue(passed.value < 1_000);
	}
}
