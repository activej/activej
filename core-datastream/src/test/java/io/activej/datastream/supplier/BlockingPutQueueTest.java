package io.activej.datastream.supplier;

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
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BlockingPutQueueTest {
	@ClassRule
	public static final EventloopRule EVENTLOOP_RULE = new EventloopRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public boolean slowPut;

	@Parameter(2)
	public boolean slowTake;

	@Parameter(3)
	public int numberOfPutThreads;

	@Parameter(4)
	public int numberOfElements;

	private CountDownLatch putThreadsLatch;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
				new Object[]{"Slow put, single put thread", true, false, 1, 1_000},
				new Object[]{"Slow take, single put thread", false, true, 1, 100_000},
				new Object[]{"Equal, single put thread", false, false, 1, 1_000_000},

				new Object[]{"Slow put, ten put threads", true, false, 10, 10_000},
				new Object[]{"Slow take, ten put threads", false, true, 10, 100_000},
				new Object[]{"Equal, ten put threads", false, false, 10, 1_000_000}
		);
	}

	@Before
	public void setUp() throws Exception {
		putThreadsLatch = new CountDownLatch(numberOfPutThreads);
	}

	@Test
	public void testStream() throws InterruptedException {
		Eventloop reactor = Reactor.getCurrentReactor();

		TestSupplierBlockingPutQueueConsumer queue = new TestSupplierBlockingPutQueueConsumer();

		PutThread[] putThreads = new PutThread[numberOfPutThreads];
		int k = numberOfElements / numberOfPutThreads;
		for (int i = 0; i < putThreads.length; i++) {
			putThreads[i] = new PutThread(queue, IntStream.range(i * k, (i + 1) * k));
		}
		for (PutThread putThread : putThreads) {
			putThread.start();
		}

		Thread waitThread = new Thread(() -> {
			try {
				putThreadsLatch.await();
				reactor.keepAlive(false);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		});
		waitThread.start();

		reactor.keepAlive(true);
		reactor.run();
		waitThread.join();

		assertEquals(numberOfElements, queue.taken.size());

		queue.taken.sort(Comparator.naturalOrder());
		for (int i = 0; i < queue.taken.size(); i++) {
			assertEquals(i, queue.taken.get(i).intValue());
		}
	}

	private final class TestSupplierBlockingPutQueueConsumer extends BlockingPutQueue<Integer> {
		private final List<Integer> taken = new ArrayList<>();

		public TestSupplierBlockingPutQueueConsumer() {
			super(8192);
		}

		@Override
		protected void onMoreData() {
			do {
				Integer number = take();

				taken.add(number);

				if (isEmpty()) {
					return;
				}

			} while (!slowTake);
		}
	}

	private final class PutThread extends Thread {
		private PutThread(TestSupplierBlockingPutQueueConsumer queue, IntStream intStream) {
			super(() -> {
				try {
					int[] ints = intStream.toArray();
					for (int number : ints) {
						queue.put(number);
						if (slowPut) {
							Thread.sleep(1);
						}
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AssertionError(e);
				}
				putThreadsLatch.countDown();
			});
		}
	}
}
