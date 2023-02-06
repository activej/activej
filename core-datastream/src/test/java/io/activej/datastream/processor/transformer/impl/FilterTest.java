package io.activej.datastream.processor.transformer.impl;

import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class FilterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6);
		StreamTransformer<Integer, Integer> filter = new Filter<>(input -> input % 2 == 1);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(supplier.transformWith(filter)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 3, 5), consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(filter);
		assertEndOfStream(consumer);
	}

	@Test
	public void testWithError() {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6);
		StreamTransformer<Integer, Integer> streamFilter = new Filter<>(input -> input % 2 != 1);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(source.transformWith(streamFilter)
				.streamTo(consumer
						.transformWith(decorate(promise ->
								promise.then(item -> item == 4 ? Promise.ofException(exception) : Promise.of(item))))));

		assertSame(exception, e);

		assertEquals(List.of(2, 4), consumer.getList());
		assertClosedWithError(source);
		assertClosedWithError(consumer);
		assertClosedWithError(streamFilter);
	}

	@Test
	public void testSupplierDisconnectWithError() {
		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<Integer> source = StreamSuppliers.concat(
				StreamSuppliers.ofIterable(List.of(1, 2, 3, 4, 5, 6)),
				StreamSuppliers.closingWithError(exception));

		StreamTransformer<Integer, Integer> streamFilter = new Filter<>(input -> input % 2 != 1);

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		Exception e = awaitException(source.transformWith(streamFilter)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(exception, e);

//		assertEquals(3, consumer.getList().size());
		assertClosedWithError(consumer);
		assertClosedWithError(streamFilter);
	}

	@Test
	public void testFilterConsumer() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6);
		StreamTransformer<Integer, Integer> filter = new Filter<>(input -> input % 2 == 1);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		StreamConsumer<Integer> transformedConsumer = consumer
				.transformWith(filter)
				.transformWith(randomlySuspending());

		await(supplier.streamTo(transformedConsumer));

		assertEquals(List.of(1, 3, 5), consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(filter);
	}

}
