package io.activej.datastream;

import io.activej.eventloop.Eventloop;
import io.activej.reactor.Reactor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class StreamSupplierBlockingTest {

	@Test
	public void testSupplier() throws InterruptedException, ExecutionException {
		Eventloop reactor = Eventloop.create();

		BlockingStreamSupplier<Integer> supplier = Reactor.executeWithReactor(reactor,
				() -> BlockingStreamSupplier.create());

		List<Integer> original = IntStream.range(0, 1000).boxed().toList();

		CompletableFuture<List<Integer>> listFuture = reactor.submit(() -> {
			ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
			return supplier.streamTo(consumer.transformWith(TestStreamTransformers.randomlySuspending()))
					.map($ -> consumer.getList());
		});

		reactor.startExternalTask();
		Thread thread = new Thread(reactor);
		thread.start();

		for (Integer integer : original) {
			supplier.put(integer);
		}
		supplier.putEndOfStream();

		reactor.completeExternalTask();

		List<Integer> result = listFuture.get();

		assertEquals(original, result);
	}

	@Test
	public void testSupplierPreemptiveAcknowledge() throws InterruptedException, ExecutionException {
		Eventloop reactor = Eventloop.create();

		BlockingStreamSupplier<Integer> supplier = Reactor.executeWithReactor(reactor,
				() -> BlockingStreamSupplier.create());

		List<Integer> original = IntStream.range(0, 1000).boxed().toList();
		List<Integer> result = new ArrayList<>();

		CompletableFuture<Void> future = reactor.submit(() ->
				supplier.streamTo(new AbstractStreamConsumer<>() {
					@Override
					protected void onInit() {
						resume(integer -> {
							result.add(integer);
							if (result.size() == 500) {
								acknowledge();
							}
						});
					}
				})
		);

		reactor.startExternalTask();
		Thread thread = new Thread(reactor);
		thread.start();

		for (Integer integer : original) {
			supplier.put(integer);
		}

		reactor.completeExternalTask();
		future.get();

		assertEquals(original.subList(0, 500), result);
	}

	@Test
	public void testSupplierWithException() throws InterruptedException {
		RuntimeException testException = new RuntimeException("Test");

		Eventloop reactor = Eventloop.create();

		BlockingStreamSupplier<Integer> supplier = Reactor.executeWithReactor(reactor,
				() -> BlockingStreamSupplier.create());

		List<Integer> original = IntStream.range(0, 1000).boxed().toList();
		List<Integer> result = new ArrayList<>();

		int errorLimit = 500;
		CountDownLatch errorLatch = new CountDownLatch(1);
		reactor.submit(() ->
				supplier.streamTo(new AbstractStreamConsumer<>() {
					@Override
					protected void onInit() {
						resume(integer -> {
							result.add(integer);
							if (result.size() == errorLimit) {
								closeEx(testException);
								errorLatch.countDown();
							}
						});
					}
				}));

		reactor.startExternalTask();
		Thread thread = new Thread(reactor);
		thread.start();

		try {
			for (int i = 0; i < errorLimit; i++) {
				supplier.put(original.get(i));
			}
			errorLatch.await();
			for (int i = errorLimit; i < original.size(); i++) {
				supplier.put(original.get(i));
			}
			fail();
		} catch (ExecutionException e) {
			reactor.completeExternalTask();
			assertSame(testException, e.getCause());
		}

		assertEquals(original.subList(0, errorLimit), result);
	}

}
