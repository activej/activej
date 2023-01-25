package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer_ToList;
import io.activej.datastream.StreamSupplier;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamLimiterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testLimit() {
		List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(original);
		StreamConsumer_ToList<Integer> consumer = StreamConsumer_ToList.create();

		int limit = 5;
		await(supplier.transformWith(StreamLimiter.create(limit)).streamTo(consumer));

		assertEquals(original.subList(0, limit), consumer.getList());
	}

	@Test
	public void testLimitZero() {
		List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(original);
		StreamConsumer_ToList<Integer> consumer = StreamConsumer_ToList.create();

		await(supplier.transformWith(StreamLimiter.create(0)).streamTo(consumer));

		assertTrue(consumer.getList().isEmpty());
	}

	@Test
	public void testNoLimit() {
		List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(original);
		StreamConsumer_ToList<Integer> consumer = StreamConsumer_ToList.create();

		await(supplier.transformWith(StreamLimiter.create(StreamLimiter.NO_LIMIT)).streamTo(consumer));

		assertEquals(original, consumer.getList());
	}
}
