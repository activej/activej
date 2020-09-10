package io.activej.async.service;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.RetryPolicy;
import io.activej.promise.SettablePromise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertFalse;

public final class EventloopTaskSchedulerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testStopping() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		SettablePromise<Void> settablePromise = new SettablePromise<>();

		EventloopTaskScheduler scheduler = EventloopTaskScheduler.create(eventloop, () -> settablePromise)
				.withSchedule(EventloopTaskScheduler.Schedule.immediate());
		scheduler.start();

		await(Promise.complete().async()
				.then(() -> {
					Promise<Void> stopPromise = scheduler.stop();
					assertFalse(stopPromise.isComplete());
					settablePromise.set(null);
					return stopPromise;
				}));

		eventloop.run();
	}

	@Test
	public void testStoppingWithRetryPolicy() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		SettablePromise<Void> settablePromise = new SettablePromise<>();

		EventloopTaskScheduler scheduler = EventloopTaskScheduler.create(eventloop, () -> Promise.ofException(new Exception()))
				.withRetryPolicy(RetryPolicy.immediateRetry())
				.withSchedule(EventloopTaskScheduler.Schedule.immediate());
		scheduler.start();

		await(Promise.complete().async()
				.then(() -> {
					Promise<Void> stopPromise = scheduler.stop();
					assertFalse(stopPromise.isComplete());
					settablePromise.set(null);
					return stopPromise;
				}));

		eventloop.run();
	}
}
