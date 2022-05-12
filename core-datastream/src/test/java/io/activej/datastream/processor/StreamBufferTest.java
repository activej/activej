package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.datastream.StreamSupplier.closingWithError;
import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamBufferTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testZeroMinSize() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(0, 1);
		await(supplier.transformWith(buffer).streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
		assertEndOfStream(supplier, consumer);
		assertEndOfStream(buffer);
	}

	@Test
	public void testBufferedSupplier() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		await(supplier.transformWith(buffer).streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
		assertEndOfStream(supplier, consumer);
		assertEndOfStream(buffer);
	}

	@Test
	public void testBufferedConsumer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		await(supplier.streamTo(consumer.transformWith(buffer).transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer.getList());
		assertEndOfStream(supplier, consumer);
		assertEndOfStream(buffer);
	}

	@Test
	public void testSupplierError() {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSupplier.concat(
				StreamSupplier.of(1, 2, 3, 4, 5),
				closingWithError(expectedException),
				StreamSupplier.of(6,7,8,9,10)
		);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		Exception exception = awaitException(supplier.streamTo(consumer.transformWith(buffer).transformWith(randomlySuspending())));
		assertSame(expectedException, exception);

		assertEquals(List.of(1, 2, 3, 4, 5), consumer.getList());
		assertClosedWithError(expectedException, supplier, consumer);
		assertClosedWithError(expectedException, buffer);
	}

	@Test
	public void testConsumerError() {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamBuffer<Integer> buffer = StreamBuffer.create(1, 2);
		Exception exception = awaitException(supplier.streamTo(consumer
				.transformWith(buffer)
				.transformWith(decorate(promise -> promise.then(
						item -> item == 5 ? Promise.ofException(expectedException) : Promise.of(item))))
				.transformWith(randomlySuspending())));
		assertSame(expectedException, exception);

		assertEquals(List.of(1, 2, 3, 4, 5), consumer.getList());
		assertClosedWithError(expectedException, supplier, consumer);
		assertClosedWithError(expectedException, buffer);
	}
}
