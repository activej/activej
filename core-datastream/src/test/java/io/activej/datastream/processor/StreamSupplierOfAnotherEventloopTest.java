package io.activej.datastream.processor;

import io.activej.common.exception.ExpectedException;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.TestUtils.CountingStreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.eventloop.Eventloop.initWithEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSupplierOfAnotherEventloopTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private Eventloop anotherEventloop;

	@Before
	public void setUp() {
		anotherEventloop = Eventloop.create();
		anotherEventloop.keepAlive(true);
		new Thread(anotherEventloop, "another").start();
	}

	@After
	public void tearDown() {
		anotherEventloop.execute(() -> anotherEventloop.keepAlive(false));
	}

	@Test
	public void testStreaming() throws ExecutionException, InterruptedException {
		StreamSupplier<Integer> anotherEventloopSupplier = initWithEventloop(anotherEventloop, () -> StreamSupplier.of(1, 2, 3, 4, 5));
		StreamSupplier<Integer> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5), consumer.getList());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopSupplier)).get();
	}

	@Test
	public void testSupplierException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> anotherEventloopSupplier = initWithEventloop(anotherEventloop, () ->
				StreamSupplier.concat(StreamSupplier.of(1, 2, 3), StreamSupplier.closingWithError(expectedException), StreamSupplier.of(4, 5, 6)));
		StreamSupplier<Integer> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
		anotherEventloop.submit(() -> assertClosedWithError(expectedException, anotherEventloopSupplier)).get();
	}

	@Test
	public void testConsumerException() {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> anotherEventloopSupplier = initWithEventloop(anotherEventloop, () -> StreamSupplier.of(1, 2, 3, 4, 5));
		StreamSupplier<Integer> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);
		StreamConsumer<Integer> consumer = StreamConsumerToList.create();

		Throwable exception = awaitException(supplier.streamTo(consumer
				.transformWith(decorate(promise -> promise
						.then(item -> item == 4 ? Promise.ofException(expectedException) : Promise.of(item))))));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
	}

	@Test
	public void testForOutOfMemoryError() throws ExecutionException, InterruptedException {
		int nItems = 10000;
		StreamSupplier<byte[]> anotherEventloopSupplier = initWithEventloop(anotherEventloop, () -> StreamSupplier.ofStream(Stream.generate(() -> new byte[1024 * 1024]).limit(nItems)));
		CountingStreamConsumer<byte[]> consumer = new CountingStreamConsumer<>();
		StreamSupplier<byte[]> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(nItems, consumer.getCount());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopSupplier)).get();
	}
}
