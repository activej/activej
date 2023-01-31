package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.ToListStreamConsumer;
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

public class StreamFilterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamFilter<Integer, Integer> filter = StreamFilter.create(input -> input % 2 == 1);
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
		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamFilter<Integer, Integer> streamFilter = StreamFilter.create(input -> input % 2 != 1);
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
		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.ofIterable(List.of(1, 2, 3, 4, 5, 6)),
				StreamSupplier.closingWithError(exception));

		StreamFilter<Integer, Integer> streamFilter = StreamFilter.create(input -> input % 2 != 1);

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
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamFilter<Integer, Integer> filter = StreamFilter.create(input -> input % 2 == 1);
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
