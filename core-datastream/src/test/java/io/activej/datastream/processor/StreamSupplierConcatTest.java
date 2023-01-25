package io.activej.datastream.processor;

import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.StreamConsumer_ToList;
import io.activej.datastream.StreamSupplier;
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

public class StreamSupplierConcatTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSequence() {
		StreamConsumer_ToList<Integer> consumer = StreamConsumer_ToList.create();

		await(StreamSupplier.concat(
						StreamSupplier.of(1, 2, 3),
						StreamSupplier.of(4, 5, 6))
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5, 6), consumer.getList());
		assertEndOfStream(consumer);
	}

	@Test
	public void testSequenceException() {
		List<Integer> list = new ArrayList<>();

		StreamConsumer_ToList<Integer> consumer = StreamConsumer_ToList.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(StreamSupplier.concat(
						StreamSupplier.of(1, 2, 3),
						StreamSupplier.of(4, 5, 6),
						StreamSupplier.closingWithError(exception),
						StreamSupplier.of(1, 2, 3))
				.streamTo(consumer));

		assertSame(exception, e);
		assertEquals(List.of(1, 2, 3, 4, 5, 6), list);
		assertClosedWithError(consumer);
	}

	@Test
	public void testConcat() {
		List<Integer> list = await(StreamSupplier.concat(
						StreamSupplier.of(1, 2, 3),
						StreamSupplier.of(4, 5, 6),
						StreamSupplier.of())
				.toList());

		assertEquals(List.of(1, 2, 3, 4, 5, 6), list);
	}

	@Test
	public void testConcatException() {
		List<Integer> list = new ArrayList<>();

		StreamConsumer_ToList<Integer> consumer = StreamConsumer_ToList.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(StreamSupplier.concat(
						StreamSupplier.of(1, 2, 3),
						StreamSupplier.of(4, 5, 6),
						StreamSupplier.closingWithError(exception))
				.streamTo(consumer));

		assertSame(exception, e);
		assertEquals(List.of(1, 2, 3, 4, 5, 6), list);

	}

	@Test
	public void testConcatPreemptiveAcknowledge() {
		List<Integer> result = new ArrayList<>();
		await(StreamSupplier.concat(
						StreamSupplier.of(1, 2, 3),
						StreamSupplier.of(4, 5, 6)
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
