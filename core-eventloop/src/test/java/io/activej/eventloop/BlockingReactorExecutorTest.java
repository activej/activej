package io.activej.eventloop;

import io.activej.async.executor.BlockingReactorExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static org.junit.Assert.assertEquals;

public final class BlockingReactorExecutorTest {
	private Eventloop eventloop;

	@Before
	public void setUp() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrow());
	}

	@Test
	public void testExecute() throws InterruptedException {
		int limit = 10;
		BlockingReactorExecutor eventloopExecutor = BlockingReactorExecutor.create(eventloop, limit);
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

		assertEquals(expected, actual);
	}

	@Test
	public void testSubmitRunnable() throws InterruptedException, ExecutionException {
		int limit = 10;
		BlockingReactorExecutor eventloopExecutor = BlockingReactorExecutor.create(eventloop, limit);
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

		assertEquals(expected, actual);
	}

	@Test
	public void testSubmitSupplier() throws ExecutionException, InterruptedException {
		int limit = 10;
		BlockingReactorExecutor eventloopExecutor = BlockingReactorExecutor.create(eventloop, limit);
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

		assertEquals(expected, actual);

	}
}
