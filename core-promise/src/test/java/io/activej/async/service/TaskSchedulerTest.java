package io.activej.async.service;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.RetryPolicy;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertFalse;

public final class TaskSchedulerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testStopping() {
		Eventloop eventloop = Reactor.getCurrentReactor();
		SettablePromise<Void> settablePromise = new SettablePromise<>();

		TaskScheduler scheduler = TaskScheduler.builder(eventloop, () -> settablePromise)
			.withSchedule(TaskScheduler.Schedule.immediate())
			.build();
		scheduler.start();

		await(Promise.complete().async()
			.then(() -> {
				Promise<?> stopPromise = scheduler.stop();
				assertFalse(stopPromise.isComplete());
				settablePromise.set(null);
				return stopPromise;
			}));

		eventloop.run();
	}

	@Test
	public void testStoppingWithRetryPolicy() {
		Eventloop eventloop = Reactor.getCurrentReactor();
		SettablePromise<Void> settablePromise = new SettablePromise<>();

		TaskScheduler scheduler = TaskScheduler.builder(eventloop, () -> Promise.ofException(new Exception()))
			.withRetryPolicy(RetryPolicy.immediateRetry())
			.withSchedule(TaskScheduler.Schedule.immediate())
			.build();
		scheduler.start();

		await(Promise.complete().async()
			.then(() -> {
				Promise<?> stopPromise = scheduler.stop();
				assertFalse(stopPromise.isComplete());
				settablePromise.set(null);
				return stopPromise;
			}));

		eventloop.run();
	}
}
