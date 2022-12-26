package io.activej.datastream;

import io.activej.eventloop.Eventloop;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class BlockingStreamSupplierTest {

	@Test
	public void testSupplier() throws InterruptedException, ExecutionException {
		Eventloop eventloop = Eventloop.create();

		BlockingStreamSupplier<Integer> supplier = Eventloop.initWithEventloop(eventloop,
				() -> BlockingStreamSupplier.create());

		List<Integer> original = IntStream.range(0, 1000).boxed().toList();

		CompletableFuture<List<Integer>> listFuture = eventloop.submit(() -> {
			StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
			return supplier.streamTo(consumer.transformWith(TestStreamTransformers.randomlySuspending()))
					.map($ -> consumer.getList());
		});

		eventloop.startExternalTask();
		Thread thread = new Thread(eventloop);
		thread.start();

		for (Integer integer : original) {
			supplier.put(integer);
		}
		supplier.putEndOfStream();

		eventloop.completeExternalTask();

		List<Integer> result = listFuture.get();

		assertEquals(original, result);
	}

	@Test
	public void testSupplierPreemptiveAcknowledge() throws InterruptedException, ExecutionException {
		Eventloop eventloop = Eventloop.create();

		BlockingStreamSupplier<Integer> supplier = Eventloop.initWithEventloop(eventloop,
				() -> BlockingStreamSupplier.create());

		List<Integer> original = IntStream.range(0, 1000).boxed().toList();
		List<Integer> result = new ArrayList<>();

		CompletableFuture<Void> future = eventloop.submit(() ->
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

		eventloop.startExternalTask();
		Thread thread = new Thread(eventloop);
		thread.start();

		for (Integer integer : original) {
			supplier.put(integer);
		}

		eventloop.completeExternalTask();
		future.get();

		assertEquals(original.subList(0, 500), result);
	}

	@Test
	public void testSupplierWithException() throws InterruptedException {
		RuntimeException testException = new RuntimeException("Test");

		Eventloop eventloop = Eventloop.create();

		BlockingStreamSupplier<Integer> supplier = Eventloop.initWithEventloop(eventloop,
				() -> BlockingStreamSupplier.create());

		List<Integer> original = IntStream.range(0, 1000).boxed().toList();
		List<Integer> result = new ArrayList<>();

		int errorLimit = 500;
		CountDownLatch errorLatch = new CountDownLatch(1);
		eventloop.submit(() ->
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

		eventloop.startExternalTask();
		Thread thread = new Thread(eventloop);
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
			eventloop.completeExternalTask();
			assertSame(testException, e.getCause());
		}

		assertEquals(original.subList(0, errorLimit), result);
	}

}
