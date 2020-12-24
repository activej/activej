package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.datastream.StreamSupplier.concat;
import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.datastream.processor.Transducer.filter;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamTransducerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testFilterTransducer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamTransducer<Integer, Integer> filter = StreamTransducer.create(filter(input -> input % 2 == 1));
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(supplier.transformWith(filter)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 3, 5), consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(filter);
	}

	@Test
	public void testMapperTransducer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamTransducer<Integer, Integer> mapper = StreamTransducer.create(Transducer.mapper(input -> input * input));

		await(supplier.transformWith(mapper)
				.streamTo(consumer.transformWith(oneByOne())));

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertEndOfStream(supplier);
		assertEndOfStream(mapper);
		assertEndOfStream(consumer);
	}


	@Test
	public void testConsumerError() {
		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamTransducer<Integer, Integer> transducer = StreamTransducer.create(new TransducerPipe<>());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Throwable e = awaitException(source.transformWith(transducer)
				.streamTo(consumer
						.transformWith(decorate(promise ->
								promise.then(item -> item == 4 ? Promise.ofException(exception) : Promise.of(item))))));

		assertSame(exception, e);

		assertEquals(asList(1, 2, 3, 4), consumer.getList());
		assertClosedWithError(source);
		assertClosedWithError(consumer);
		assertClosedWithError(transducer);
	}

	@Test
	public void testSupplierError() {
		StreamTransducer<Integer, Integer> transducer = StreamTransducer.create(new TransducerPipe<>());

		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<Integer> supplier = concat(
				StreamSupplier.of(1, 2, 3),
				StreamSupplier.of(4, 5, 6),
				StreamSupplier.closingWithError(exception));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable e = awaitException(supplier.transformWith(transducer)
				.streamTo(consumer));

		assertSame(exception, e);
		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());

		assertClosedWithError(consumer);
		assertClosedWithError(transducer);
	}

	@Test
	public void testConsecutiveTransducers() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamTransducer<Integer, Integer> mapper = StreamTransducer.create(Transducer.mapper(x -> x * x));
		StreamTransducer<Integer, Integer> filter = StreamTransducer.create(filter(x -> x % 2 == 0));

		await(supplier.transformWith(mapper).transformWith(filter)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(4, 16, 36), consumer.getList());

		assertEndOfStream(supplier);
		assertEndOfStream(mapper);
		assertEndOfStream(filter);
		assertEndOfStream(consumer);
	}

	private static class TransducerPipe<T, A> extends AbstractTransducer<T, T, A> {
		@Override
		public void onItem(StreamDataAcceptor<T> output, T item, A accumulator) {
			output.accept(item);
		}
	}
}
