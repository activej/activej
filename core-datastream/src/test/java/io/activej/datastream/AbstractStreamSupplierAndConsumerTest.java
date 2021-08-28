package io.activej.datastream;

import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.activej.datastream.AbstractStreamSupplierAndConsumerTest.Status.*;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public final class AbstractStreamSupplierAndConsumerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final ExpectedException EXPECTED_EXCEPTION = new ExpectedException("test");
	private static final Random RANDOM = new Random();

	private StatusAssertingSupplier supplier;
	private StatusAssertingConsumer consumer;
	private AssertingDataAcceptor dataAcceptor;

	@Before
	public void setUp() {
		supplier = new StatusAssertingSupplier();
		consumer = new StatusAssertingConsumer();
		dataAcceptor = new AssertingDataAcceptor();
	}

	@After
	public void tearDown() {
		assertTrue(supplier.isEndOfStream());
		assertTrue(consumer.isEndOfStream());
	}

	@Test
	public void testSendingBeforeBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySend(4);
		supplier.trySend(5);
		await(supplier.trySendEndOfStream(), streamPromise);
		consumer.resume(consumer.getDataAcceptor());

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, START, RESUME_SUSPEND, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testSendingAfterBound() {
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		await(supplier.trySendEndOfStream(), streamPromise);

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, START, RESUME_SUSPEND, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testCompletedStreamBeforeBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		await(supplier.trySendEndOfStream(), supplier.streamTo(consumer));

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingBeforeBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		closeEither();
		Exception exception = awaitException(supplier.streamTo(consumer));

		assertSame(EXPECTED_EXCEPTION, exception);
		assertClosedWithError(EXPECTED_EXCEPTION, supplier, consumer);

		supplier.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingAfterBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		closeEither();
		Exception exception = awaitException(streamPromise);

		assertSame(EXPECTED_EXCEPTION, exception);
		assertClosedWithError(EXPECTED_EXCEPTION, supplier, consumer);

		supplier.assertStatuses(INIT, START, RESUME_SUSPEND, ERROR, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, ERROR, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingAfterEndOfStreamBeforeBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		supplier.trySendEndOfStream();
		closeEither();
		Exception exception = awaitException(supplier.streamTo(consumer));

		assertSame(EXPECTED_EXCEPTION, exception);
		assertClosedWithError(EXPECTED_EXCEPTION, supplier, consumer);

		supplier.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingWhileSendingEndOfStreamAfterBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		consumer.suspend();
		supplier.sendEndOfStream();
		Promise<Void> streamPromise = supplier.streamTo(consumer);

		closeEither();
		consumer.resume(dataAcceptor);
		Exception exception = awaitException(streamPromise);

		assertSame(EXPECTED_EXCEPTION, exception);
		assertClosedWithError(EXPECTED_EXCEPTION, supplier, consumer);

		supplier.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, ERROR, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingAfterEndOfStreamAfterBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		consumer.resume(dataAcceptor);
		supplier.sendEndOfStream();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		closeEither();

		await(streamPromise);

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testSendingAfterEndOfStreamBeforeBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		supplier.trySendEndOfStream();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySend(6);
		supplier.trySend(7);
		await(streamPromise);

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testSendingAfterEndOfStreamAfterBound() {
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		supplier.trySendEndOfStream();
		supplier.trySend(6);
		supplier.trySend(7);
		await(streamPromise);

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, START, RESUME_SUSPEND, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testSendingEndOfStreamAfterEndOfStreamBeforeBound() {
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		supplier.trySendEndOfStream();
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySendEndOfStream();
		supplier.trySendEndOfStream();
		supplier.trySendEndOfStream();
		await(streamPromise);

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testSendingEndOfStreamAfterEndOfStreamAfterBound() {
		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		supplier.trySendEndOfStream();
		supplier.trySendEndOfStream();
		supplier.trySendEndOfStream();
		supplier.trySendEndOfStream();
		await(streamPromise);

		dataAcceptor.assertAccepted(5);
		assertEndOfStream(supplier, consumer);

		supplier.assertStatuses(INIT, START, RESUME_SUSPEND, ACKNOWLEDGE, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, END_OF_STREAM, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingAfterClosedBeforeBound() {
		ExpectedException secondException = new ExpectedException("second");

		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		supplier.closeEx(EXPECTED_EXCEPTION);
		supplier.closeEx(secondException);
		Exception exception = awaitException(supplier.streamTo(consumer));

		assertSame(EXPECTED_EXCEPTION, exception);
		assertClosedWithError(EXPECTED_EXCEPTION, supplier, consumer);

		supplier.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, ERROR, COMPLETE, CLEANUP);
	}

	@Test
	public void testClosingAfterClosedAfterBound() {
		ExpectedException secondException = new ExpectedException("second");

		Promise<Void> streamPromise = supplier.streamTo(consumer);
		supplier.trySend(1);
		supplier.trySend(2);
		supplier.trySend(3);
		supplier.trySend(4);
		supplier.trySend(5);
		consumer.closeEx(EXPECTED_EXCEPTION);
		consumer.closeEx(secondException);
		Exception exception = awaitException(streamPromise);

		assertSame(EXPECTED_EXCEPTION, exception);
		assertClosedWithError(EXPECTED_EXCEPTION, supplier, consumer);

		supplier.assertStatuses(INIT, START, RESUME_SUSPEND, ERROR, COMPLETE, CLEANUP);
		consumer.assertStatuses(INIT, START, ERROR, COMPLETE, CLEANUP);
	}

	private void closeEither() {
		if (RANDOM.nextBoolean()) {
			supplier.closeEx(EXPECTED_EXCEPTION);
		} else {
			consumer.closeEx(EXPECTED_EXCEPTION);
		}
	}

	// region stubs
	static final class AssertingDataAcceptor implements StreamDataAcceptor<Integer> {
		int current;

		@Override
		public void accept(Integer item) {
			assertEquals(Integer.valueOf(++current), item);
		}

		public void assertAccepted(int numberOfItems) {
			assertEquals(numberOfItems, current);
		}
	}

	enum Status {
		INIT, START, RESUME, SUSPEND, RESUME_SUSPEND, ACKNOWLEDGE, END_OF_STREAM, ERROR, COMPLETE, CLEANUP
	}

	private final class StatusAssertingSupplier extends AbstractStreamSupplier<Integer> {
		final List<Status> statuses = new ArrayList<>();

		public void trySend(Integer item) {
			if (RANDOM.nextBoolean()) {
				consumer.suspend();
			} else {
				consumer.resume(dataAcceptor);
			}
			send(item);
		}

		public Promise<Void> trySendEndOfStream() {
			eventloop.post(() -> consumer.resume(dataAcceptor));
			return sendEndOfStream();
		}

		void assertStatuses(Status... expectedStatuses) {
			int i = 0;
			for (Status expectedStatus : expectedStatuses) {
				if (expectedStatus == RESUME_SUSPEND) {
					Status status = statuses.get(i);
					while (status == RESUME || status == SUSPEND) {
						status = statuses.get(++i);
					}
				} else {
					assertEquals(expectedStatus, statuses.get(i++));
				}
			}
			assertEquals(i, statuses.size());
		}

		@Override
		protected void onInit() {
			statuses.add(INIT);
		}

		@Override
		protected void onStarted() {
			statuses.add(START);
		}

		@Override
		protected void onResumed() {
			statuses.add(RESUME);
		}

		@Override
		protected void onSuspended() {
			statuses.add(SUSPEND);
		}

		@Override
		protected void onAcknowledge() {
			statuses.add(ACKNOWLEDGE);
		}

		@Override
		protected void onError(Exception e) {
			statuses.add(ERROR);
		}

		@Override
		protected void onComplete() {
			statuses.add(COMPLETE);
		}

		@Override
		protected void onCleanup() {
			statuses.add(CLEANUP);
		}
	}

	private static final class StatusAssertingConsumer extends AbstractStreamConsumer<Integer> {
		final List<Status> statuses = new ArrayList<>();

		void assertStatuses(Status... expectedStatuses) {
			assertEquals(asList(expectedStatuses), statuses);
		}

		@Override
		protected void onInit() {
			statuses.add(INIT);
		}

		@Override
		protected void onStarted() {
			statuses.add(START);
		}

		@Override
		protected void onEndOfStream() {
			statuses.add(END_OF_STREAM);
			acknowledge();
		}

		@Override
		protected void onError(Exception e) {
			statuses.add(ERROR);
		}

		@Override
		protected void onComplete() {
			statuses.add(COMPLETE);
		}

		@Override
		protected void onCleanup() {
			statuses.add(CLEANUP);
		}
	}
	// endregion
}
