package io.activej.datastream.processor;

import io.activej.common.exception.CloseException;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamConsumerOfPromiseTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testOfPromise() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> delayedConsumer = StreamConsumerToList.create();
		StreamConsumer<Integer> consumer = StreamConsumer.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEndOfStream(supplier, consumer);
		assertEndOfStream(delayedConsumer);
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), delayedConsumer.getList());
	}

	@Test
	public void testClosedImmediately() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> delayedConsumer = StreamConsumerToList.create();
		StreamConsumer<Integer> consumer = StreamConsumer.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		consumer.close();
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertThat(exception, instanceOf(CloseException.class));
		assertClosedWithError(CloseException.class, supplier, consumer);
		assertClosedWithError(CloseException.class, delayedConsumer);
		assertEquals(0, delayedConsumer.getList().size());
	}

	@Test
	public void testClosedDelayedConsumer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		StreamConsumerToList<Integer> delayedConsumer = StreamConsumerToList.create();
		delayedConsumer.close();
		StreamConsumer<Integer> consumer = StreamConsumer.ofPromise(Promise.complete().async().map($ -> delayedConsumer));
		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertThat(exception, instanceOf(CloseException.class));
		assertClosedWithError(CloseException.class, supplier, consumer);
		assertClosedWithError(CloseException.class, delayedConsumer);
		assertEquals(0, delayedConsumer.getList().size());
	}
}
