package io.activej.datastream;

import io.activej.common.ref.RefInt;
import io.activej.promise.Promises;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.datastream.TestStreamTransformers.randomlySuspending;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.*;

public class StreamConsumerSwitcherTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSwitching() {
		List<StreamConsumerToList<Integer>> consumers = IntStream.range(0, 10)
				.mapToObj($ -> StreamConsumerToList.<Integer>create())
				.toList();
		StreamConsumerSwitcher<Integer> switcher = StreamConsumerSwitcher.create();

		AbstractStreamSupplier<Integer> streamSupplier = new AbstractStreamSupplier<>() {
			final RefInt refInt = new RefInt(0);
			final Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();

			@Override
			protected void onStarted() {
				switcher.switchTo(iterator.next().transformWith(randomlySuspending()));
			}

			@Override
			protected void onResumed() {
				while (isReady()) {
					send(refInt.inc());
					if (refInt.get() == 10) {
						if (iterator.hasNext()) {
							refInt.set(0);
							switcher.switchTo(iterator.next().transformWith(randomlySuspending()));
						} else {
							break;
						}
					}
				}
				if (!iterator.hasNext() && refInt.get() == 10) {
					sendEndOfStream();
				}
			}
		};

		await(streamSupplier.streamTo(switcher));

		assertEndOfStream(streamSupplier, switcher);
		consumers.forEach(consumer -> {
			assertEquals(List.of(1,2,3,4,5,6,7,8,9,10), consumer.getList());
			assertEndOfStream(consumer);
		});
	}

	@Test
	public void testSwitchingToClosedStream() {
		ExpectedException expectedException = new ExpectedException();

		StreamConsumerSwitcher<Integer> switcher = StreamConsumerSwitcher.create();
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumer<Integer> consumerClosed = StreamConsumer.closingWithError(expectedException);
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		AbstractStreamSupplier<Integer> streamSupplier = new AbstractStreamSupplier<>() {
			final RefInt refInt = new RefInt(0);

			@Override
			protected void onStarted() {
				switcher.switchTo(consumer1.transformWith(randomlySuspending()));
			}

			@Override
			protected void onResumed() {
				while (isReady()) {
					send(refInt.inc());
					int number = refInt.get();
					if (number % 10 == 0) {
						if (number == 10) {
							switcher.switchTo(consumerClosed.transformWith(randomlySuspending()));
						} else if (number == 20) {
							switcher.switchTo(consumer2.transformWith(randomlySuspending()));
						} else {
							break;
						}
					}
				}
				if (refInt.get() == 30) {
					sendEndOfStream();
				}
			}
		};

		Exception exception = awaitException(streamSupplier.streamTo(switcher));

		assertSame(expectedException, exception);
		assertClosedWithError(expectedException, streamSupplier, switcher);

//		assertClosedWithError(consumer1);
		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), consumer1.getList());

		assertClosedWithError(expectedException, consumerClosed);
		assertFalse(consumer2.isStarted());
	}

	@Test
	public void testSwitchingAfterSwitcherIsDone() {
		StreamConsumerSwitcher<Integer> switcher = StreamConsumerSwitcher.create();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		switcher.switchTo(consumer);

		await(StreamSupplier.of(1, 2, 3, 4).streamTo(switcher));

		assertEndOfStream(switcher);
		assertEndOfStream(consumer);

		assertEquals(List.of(1, 2, 3, 4), consumer.getList());
	}

	@Test
	public void testSwitchingAfterSwitcherIsClosed() {
		ExpectedException expectedException = new ExpectedException();

		StreamConsumerSwitcher<Integer> switcher = StreamConsumerSwitcher.create();
		switcher.closeEx(expectedException);

		assertSame(expectedException, awaitException(StreamSupplier.of(1, 2, 3, 4).streamTo(switcher)));

		assertClosedWithError(expectedException, switcher);
	}

	@Test
	public void testSwitchingBeforeSwitcherIsBound() {
		StreamConsumerSwitcher<Integer> switcher = StreamConsumerSwitcher.create();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		switcher.switchTo(consumer);

		await(StreamSupplier.of(1, 2, 3, 4).streamTo(switcher));

		assertEndOfStream(switcher);
		assertEndOfStream(consumer);

		assertEquals(List.of(1, 2, 3, 4), consumer.getList());
	}

	@Test
	public void testSwitchingToSlowConsumer() {
		ArrayList<Integer> list1 = new ArrayList<>();
		ArrayList<Integer> list2 = new ArrayList<>();
		List<StreamConsumer<Integer>> consumers = List.of(StreamConsumerToList.create(list1),
				StreamConsumer.ofPromise(Promises.delay(Duration.ofMillis(1), StreamConsumerToList.create(list2))));
		StreamConsumerSwitcher<Integer> switcher = StreamConsumerSwitcher.create();

		AbstractStreamSupplier<Integer> streamSupplier = new AbstractStreamSupplier<>() {
			final RefInt refInt = new RefInt(0);
			final Iterator<StreamConsumer<Integer>> iterator = consumers.iterator();

			@Override
			protected void onStarted() {
				switcher.switchTo(iterator.next());
			}

			@Override
			protected void onResumed() {
				while (isReady()) {
					send(refInt.inc());
					if (refInt.get() == 10) {
						if (iterator.hasNext()) {
							refInt.set(0);
							switcher.switchTo(iterator.next());
						} else {
							break;
						}
					}
				}
				if (!iterator.hasNext() && refInt.get() == 10) {
					sendEndOfStream();
				}
			}
		};

		await(streamSupplier.streamTo(switcher));

		assertEndOfStream(streamSupplier, switcher);

		List<Integer> expected = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		assertEquals(expected, list1);
		assertEquals(expected, list2);
	}
}
