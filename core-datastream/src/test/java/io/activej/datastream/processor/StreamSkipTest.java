package io.activej.datastream.processor;

import io.activej.datastream.StreamSupplier;
import io.activej.datastream.ToListStreamConsumer;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSkipTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSkip() {
		List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(original);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(supplier.transformWith(StreamSkip.create(3)).streamTo(consumer));

		assertEquals(original.subList(3, original.size()), consumer.getList());
	}

	@Test
	public void testNoSkip() {
		List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(original);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(supplier.transformWith(StreamSkip.create(StreamSkip.NO_SKIP)).streamTo(consumer));

		assertEquals(original, consumer.getList());
	}

	@Test
	public void testSkipAll() {
		List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(original);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(supplier.transformWith(StreamSkip.create(100)).streamTo(consumer));

		assertTrue(consumer.getList().isEmpty());
	}
}
