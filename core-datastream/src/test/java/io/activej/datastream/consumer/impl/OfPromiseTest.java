package io.activej.datastream.consumer.impl;

import io.activej.async.exception.AsyncCloseException;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class OfPromiseTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testOfPromise() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		ToListStreamConsumer<Integer> delayedConsumer = ToListStreamConsumer.create();
		StreamConsumer<Integer> consumer = StreamConsumers.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEndOfStream(supplier, consumer);
		assertEndOfStream(delayedConsumer);
		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), delayedConsumer.getList());
	}

	@Test
	public void testClosedImmediately() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		ToListStreamConsumer<Integer> delayedConsumer = ToListStreamConsumer.create();
		StreamConsumer<Integer> consumer = StreamConsumers.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		consumer.close();
		Exception exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertThat(exception, instanceOf(AsyncCloseException.class));
		assertClosedWithError(AsyncCloseException.class, supplier, consumer);
		assertClosedWithError(AsyncCloseException.class, delayedConsumer);
		assertEquals(0, delayedConsumer.getList().size());
	}

	@Test
	public void testClosedDelayedConsumer() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		ToListStreamConsumer<Integer> delayedConsumer = ToListStreamConsumer.create();
		delayedConsumer.close();
		StreamConsumer<Integer> consumer = StreamConsumers.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		Exception exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertThat(exception, instanceOf(AsyncCloseException.class));
		assertClosedWithError(AsyncCloseException.class, supplier, consumer);
		assertClosedWithError(AsyncCloseException.class, delayedConsumer);
		assertEquals(0, delayedConsumer.getList().size());
	}
}
