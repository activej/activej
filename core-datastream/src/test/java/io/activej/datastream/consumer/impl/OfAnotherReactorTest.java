package io.activej.datastream.consumer.impl;

import io.activej.datastream.TestUtils.CountingStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
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

public class OfAnotherReactorTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private Eventloop anotherEventloop;

	@Before
	public void setUp() throws InterruptedException {
		anotherEventloop = Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.build();
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
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5);
		ToListStreamConsumer<Integer> listConsumer = fromAnotherEventloop(ToListStreamConsumer::create);
		StreamConsumer<Integer> consumer = StreamConsumers.ofAnotherReactor(anotherEventloop, listConsumer);

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5), listConsumer.getList());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(listConsumer)).get();
	}

	@Test
	public void testSupplierException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSuppliers.concat(StreamSuppliers.ofValues(1, 2, 3), StreamSuppliers.closingWithError(expectedException), StreamSuppliers.ofValues(4, 5, 6));
		ToListStreamConsumer<Integer> listConsumer = fromAnotherEventloop(ToListStreamConsumer::create);
		StreamConsumer<Integer> consumer = StreamConsumers.ofAnotherReactor(anotherEventloop, listConsumer);

		Exception exception = awaitException(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
		anotherEventloop.submit(() -> assertClosedWithError(expectedException, listConsumer)).get();
	}

	@Test
	public void testConsumerException() throws ExecutionException, InterruptedException {
		ExpectedException expectedException = new ExpectedException();
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5);
		ToListStreamConsumer<Integer> listConsumer = fromAnotherEventloop(ToListStreamConsumer::create);
		StreamConsumer<Integer> consumer = StreamConsumers.ofAnotherReactor(anotherEventloop, listConsumer)
				.transformWith(decorate(promise -> promise
						.then(item -> item == 4 ? Promise.ofException(expectedException) : Promise.of(item))))
				.transformWith(randomlySuspending());

		Exception exception = awaitException(supplier.streamTo(consumer));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, supplier, consumer);
		anotherEventloop.submit(() -> assertClosedWithError(expectedException, listConsumer)).get();
	}

	@Test
	@Ignore
	public void testForOutOfMemoryError() throws ExecutionException, InterruptedException {
		int nItems = 10000;
		StreamSupplier<byte[]> supplier = StreamSuppliers.ofStream(Stream.generate(() -> new byte[1024 * 1024]).limit(nItems));
		CountingStreamConsumer<byte[]> anotherEventloopConsumer = fromAnotherEventloop(CountingStreamConsumer::new);
		StreamConsumer<byte[]> consumer = StreamConsumers.ofAnotherReactor(anotherEventloop, anotherEventloopConsumer);

		await(supplier.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(nItems, anotherEventloopConsumer.getCount());
		assertEndOfStream(supplier, consumer);
		anotherEventloop.submit(() -> assertEndOfStream(anotherEventloopConsumer)).get();
	}

	private <T> T fromAnotherEventloop(Supplier<T> supplier) throws ExecutionException, InterruptedException {
		return anotherEventloop.<T>submit(() -> cb -> cb.accept(supplier.get(), null)).get();
	}
}
