package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.TestUtils.CountingStreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSupplierOfAnotherEventloopTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private Eventloop anotherEventloop;

	@Before
	public void setUp() throws InterruptedException {
		anotherEventloop = Eventloop.create().withEventloopFatalErrorHandler(rethrow());
		anotherEventloop.keepAlive(true);
		CountDownLatch latch = new CountDownLatch(1);
		new Thread(() -> {
			anotherEventloop.post(latch::countDown);
			anotherEventloop.run();
		}, "another").start();
		latch.await();
	}

	@After
	public void tearDown() {
		anotherEventloop.execute(() -> anotherEventloop.keepAlive(false));
	}

	@Test
	public void testStreaming() throws ExecutionException, InterruptedException {
		StreamSupplier<Integer> anotherEventloopSupplier = fromAnotherEventloop(() -> StreamSupplier.of(1, 2, 3, 4, 5));
		StreamSupplier<Integer> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5), consumer.getList());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopSupplier)).get();
	}

	@Test
	public void testSupplierException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> anotherEventloopSupplier = fromAnotherEventloop(() ->
				StreamSupplier.concat(StreamSupplier.of(1, 2, 3), StreamSupplier.closingWithError(expectedException), StreamSupplier.of(4, 5, 6)));
		StreamSupplier<Integer> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Exception exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
		anotherEventloop.submit(() -> assertClosedWithError(expectedException, anotherEventloopSupplier)).get();
	}

	@Test
	public void testConsumerException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> anotherEventloopSupplier = fromAnotherEventloop(() -> StreamSupplier.of(1, 2, 3, 4, 5));
		StreamSupplier<Integer> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);
		StreamConsumer<Integer> consumer = StreamConsumerToList.create();

		Exception exception = awaitException(supplier.streamTo(consumer
				.transformWith(decorate(promise -> promise
						.then(item -> item == 4 ? Promise.ofException(expectedException) : Promise.of(item))))));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
	}

	@Test
	@Ignore
	public void testForOutOfMemoryError() throws ExecutionException, InterruptedException {
		int nItems = 10000;
		StreamSupplier<byte[]> anotherEventloopSupplier = fromAnotherEventloop(() -> StreamSupplier.ofStream(Stream.generate(() -> new byte[1024 * 1024]).limit(nItems)));
		CountingStreamConsumer<byte[]> consumer = new CountingStreamConsumer<>();
		StreamSupplier<byte[]> supplier = StreamSupplier.ofAnotherEventloop(anotherEventloop, anotherEventloopSupplier);

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(nItems, consumer.getCount());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopSupplier)).get();
	}

	private <T> T fromAnotherEventloop(Supplier<T> supplier) throws ExecutionException, InterruptedException {
		return anotherEventloop.<T>submit(() -> cb -> cb.accept(supplier.get(), null)).get();
	}
}
