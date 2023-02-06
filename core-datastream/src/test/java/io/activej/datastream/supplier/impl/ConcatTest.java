package io.activej.datastream.supplier.impl;

import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ConcatTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSequence() {
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(StreamSuppliers.concat(
						StreamSuppliers.ofValues(1, 2, 3),
						StreamSuppliers.ofValues(4, 5, 6))
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5, 6), consumer.getList());
		assertEndOfStream(consumer);
	}

	@Test
	public void testSequenceException() {
		List<Integer> list = new ArrayList<>();

		StreamConsumer<Integer> consumer = ToListStreamConsumer.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(StreamSuppliers.concat(
						StreamSuppliers.ofValues(1, 2, 3),
						StreamSuppliers.ofValues(4, 5, 6),
						StreamSuppliers.closingWithError(exception),
						StreamSuppliers.ofValues(1, 2, 3))
				.streamTo(consumer));

		assertSame(exception, e);
		assertEquals(List.of(1, 2, 3, 4, 5, 6), list);
		assertClosedWithError(consumer);
	}

	@Test
	public void testConcat() {
		List<Integer> list = await(StreamSuppliers.concat(
						StreamSuppliers.ofValues(1, 2, 3),
						StreamSuppliers.ofValues(4, 5, 6),
						StreamSuppliers.empty())
				.toList());

		assertEquals(List.of(1, 2, 3, 4, 5, 6), list);
	}

	@Test
	public void testConcatException() {
		List<Integer> list = new ArrayList<>();

		StreamConsumer<Integer> consumer = ToListStreamConsumer.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(StreamSuppliers.concat(
						StreamSuppliers.ofValues(1, 2, 3),
						StreamSuppliers.ofValues(4, 5, 6),
						StreamSuppliers.closingWithError(exception))
				.streamTo(consumer));

		assertSame(exception, e);
		assertEquals(List.of(1, 2, 3, 4, 5, 6), list);

	}

	@Test
	public void testConcatPreemptiveAcknowledge() {
		List<Integer> result = new ArrayList<>();
		await(StreamSuppliers.concat(
						StreamSuppliers.ofValues(1, 2, 3),
						StreamSuppliers.ofValues(4, 5, 6)
				)
				.streamTo(new AbstractStreamConsumer<>() {
					@Override
					protected void onInit() {
						resume(integer -> {
							result.add(integer);
							if (result.size() == 2) {
								acknowledge();
							}
						});
					}
				}));

		assertEquals(List.of(1, 2), result);
	}

}
