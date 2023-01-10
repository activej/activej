package io.activej.datastream;

import io.activej.eventloop.Eventloop;
import io.activej.reactor.Reactor;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ReactiveBlockingTakeQueueTest {
	@ClassRule
	public static final EventloopRule EVENTLOOP_RULE = new EventloopRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public boolean slowPut;

	@Parameter(2)
	public boolean slowTake;

	@Parameter(3)
	public int numberOfTakeThreads;

	@Parameter(4)
	public int numberOfElements;

	private CountDownLatch latch;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
				new Object[]{"Slow put, single take thread", true, false, 1, 100_000},
				new Object[]{"Slow take, single take thread", false, true, 1, 1_000},
				new Object[]{"Equal, single take thread", false, false, 1, 5_000_000},

				new Object[]{"Slow put, ten take threads", true, false, 10, 100_000},
				new Object[]{"Slow take, ten take threads", false, true, 10, 10_000},
				new Object[]{"Equal, ten take threads", false, false, 10, 5_000_000}
		);
	}

	@Before
	public void setUp() throws Exception {
		latch = new CountDownLatch(numberOfTakeThreads);
	}

	@Test
	public void testStream() throws InterruptedException {
		Eventloop reactor = Reactor.getCurrentReactor();
		TestTakeReactiveBlockingTakeQueue queue = new TestTakeReactiveBlockingTakeQueue();

		TakeThread[] takeThreads = new TakeThread[numberOfTakeThreads];
		for (int i = 0; i < takeThreads.length; i++) {
			takeThreads[i] = new TakeThread(queue);
		}
		for (TakeThread takeThread : takeThreads) {
			takeThread.start();
		}
		queue.onRequestMoreData();

		new Thread(() -> {
			try {
				latch.await();
				reactor.keepAlive(false);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}).start();

		reactor.keepAlive(true);
		reactor.run();
		for (TakeThread takeThread : takeThreads) {
			takeThread.join();
		}

		List<Integer> total = new ArrayList<>();
		for (TakeThread takeThread : takeThreads) {
			List<Integer> numbers = takeThread.numbers;
			List<Integer> copy = new ArrayList<>(numbers);
			copy.sort(Comparator.naturalOrder());
			assertEquals(numbers, copy);

			total.addAll(numbers);
		}

		assertEquals(numberOfElements, total.size());

		total.sort(Comparator.naturalOrder());
		for (int i = 0; i < total.size(); i++) {
			assertEquals(i, total.get(i).intValue());
		}
	}

	private final class TestTakeReactiveBlockingTakeQueue extends ReactiveBlockingTakeQueue<Integer> {
		private int i;

		public TestTakeReactiveBlockingTakeQueue() {
			super(8192);
		}

		@Override
		protected void onRequestMoreData() {
			do {
				int x = i++;
				if (x == numberOfElements) {
					endOfStream();
					return;
				}

				if (put(x)) {
					return;
				}

			} while (!slowPut);
		}
	}

	private final class TakeThread extends Thread {
		private final List<Integer> numbers;

		private TakeThread(TestTakeReactiveBlockingTakeQueue queue, List<Integer> numbers) {
			super(() -> {
				try {
					while (true) {
						Integer number = queue.take();
						if (number == null) {
							latch.countDown();
							return;
						}
						numbers.add(number);
						if (slowTake) {
							//noinspection BusyWait
							Thread.sleep(1);
						}
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AssertionError(e);
				}
			});

			this.numbers = numbers;
		}

		public TakeThread(TestTakeReactiveBlockingTakeQueue queue) {
			this(queue, new ArrayList<>());
		}
	}
}
