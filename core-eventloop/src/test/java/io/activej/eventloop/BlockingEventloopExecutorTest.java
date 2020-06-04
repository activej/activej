package io.activej.eventloop;

import io.activej.eventloop.executor.BlockingEventloopExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;

public final class BlockingEventloopExecutorTest {
	private Eventloop eventloop;

	@Before
	public void setUp() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	}

	@Test
	public void testExecute() throws InterruptedException {
		int limit = 10;
		BlockingEventloopExecutor eventloopExecutor = BlockingEventloopExecutor.create(eventloop, limit);
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		List<Integer> actual = new ArrayList<>();
		List<Integer> expected = new ArrayList<>();

		for (int i = 0; i < 20; i++) {
			int finalI = i;
			expected.add(i);
			eventloopExecutor.execute(() -> actual.add(finalI));
		}

		eventloopExecutor.execute(() -> eventloop.keepAlive(false));

		eventloopThread.join();

		assertEquals(actual, expected);
	}

	@Test
	public void testSubmitRunnable() throws InterruptedException, ExecutionException {
		int limit = 10;
		BlockingEventloopExecutor eventloopExecutor = BlockingEventloopExecutor.create(eventloop, limit);
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		List<Integer> actual = new ArrayList<>();
		List<Integer> expected = new ArrayList<>();

		for (int i = 0; i < 20; i++) {
			int finalI = i;
			expected.add(i);
			eventloopExecutor.submit(() -> actual.add(finalI)).get();
		}

		eventloopExecutor.execute(() -> eventloop.keepAlive(false));

		eventloopThread.join();

		assertEquals(actual, expected);
	}

	@Test
	public void testSubmitSupplier() throws ExecutionException, InterruptedException {
		int limit = 10;
		BlockingEventloopExecutor eventloopExecutor = BlockingEventloopExecutor.create(eventloop, limit);
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		List<Integer> actual = new ArrayList<>();
		List<Integer> expected = new ArrayList<>();

		for (int i = 0; i < 20; i++) {
			int finalI = i;
			expected.add(i);
			Integer actualI = eventloopExecutor.<Integer>submit(() -> cb -> cb.accept(finalI, null)).get();
			actual.add(actualI);
		}

		eventloopExecutor.execute(() -> eventloop.keepAlive(false));

		eventloopThread.join();

		assertEquals(actual, expected);

	}
}
