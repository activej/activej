package io.activej.async;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.ref.Ref;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.async.function.AsyncSuppliers.coalesce;
import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class AsyncSuppliersTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testReuse() {
		AsyncSupplier<Void> reuse = reuse(() -> Promise.complete().async());

		Promise<Void> promise1 = reuse.get();
		Promise<Void> promise2 = reuse.get();
		Promise<Void> promise3 = reuse.get();
		Promise<Void> promise4 = reuse.get();

		assertSame(promise1, promise2);
		assertSame(promise2, promise3);
		assertSame(promise3, promise4);
	}

	@Test
	public void subscribeNormalUsage() {
		AsyncSupplier<Void> subscribe = coalesce(() -> Promise.complete().async());

		Promise<Void> promise1 = subscribe.get();

		Promise<Void> promise2 = subscribe.get();
		Promise<Void> promise3 = subscribe.get();
		Promise<Void> promise4 = subscribe.get();

		assertNotSame(promise1, promise2);

		assertSame(promise2, promise3);
		assertSame(promise2, promise4);
	}

	@Test
	public void subscribeIfGetAfterFirstPromise() {
		AsyncSupplier<Void> subscribe = coalesce(() -> Promise.complete().async());

		Ref<Promise<Void>> nextPromiseRef = new Ref<>();
		Promise<Void> promise1 = subscribe.get()
				.whenComplete(() -> nextPromiseRef.value = subscribe.get());

		Promise<Void> promise2 = subscribe.get();
		Promise<Void> promise3 = subscribe.get();
		Promise<Void> promise4 = subscribe.get();

		await(promise1);

		assertNotSame(promise1, promise2);

		assertSame(promise2, promise3);
		assertSame(promise2, promise4);

		// subscribed to secondly returned promise
		assertNotSame(nextPromiseRef.value, promise1);
		assertSame(nextPromiseRef.value, promise2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void subscribeIfGetAfterFirstPromiseRecursive() {
		AsyncSupplier<Void> subscribe = coalesce(() -> Promise.complete().async());

		Promise<Void>[] nextPromise = new Promise[3];
		Promise<Void> promise1 = subscribe.get()
				.whenComplete(() -> nextPromise[0] = subscribe.get()
						.whenComplete(() -> nextPromise[1] = subscribe.get()
								.whenComplete(() -> nextPromise[2] = subscribe.get())));

		Promise<Void> promise2 = subscribe.get();
		Promise<Void> promise3 = subscribe.get();
		Promise<Void> promise4 = subscribe.get();

		await(promise1);

		assertNotSame(promise1, promise2);

		assertSame(promise2, promise3);
		assertSame(promise2, promise4);

		// first recursion subscribed to secondly returned promise
		assertNotSame(nextPromise[0], promise1);
		assertSame(nextPromise[0], promise2);

		// next recursions subscribed to newly created promises
		assertNotSame(nextPromise[0], nextPromise[1]);
		assertNotSame(nextPromise[1], nextPromise[2]);
	}

	@Test
	public void subscribeIfGetAfterLaterPromises() {
		AsyncSupplier<Void> subscribe = coalesce(() -> Promise.complete().async());

		Ref<Promise<Void>> nextPromiseRef = new Ref<>();
		Promise<Void> promise1 = subscribe.get();

		Promise<Void> promise2 = subscribe.get();
		Promise<Void> promise3 = subscribe.get()
				.whenComplete(() -> nextPromiseRef.value = subscribe.get());
		Promise<Void> promise4 = subscribe.get();

		await(promise1);

		assertNotSame(promise1, promise2);

		assertSame(promise2, promise3);
		assertSame(promise2, promise4);

		// subscribed to new promise
		assertNotSame(nextPromiseRef.value, promise1);
		assertNotSame(nextPromiseRef.value, promise2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void subscribeIfGetAfterLaterPromisesRecursive() {
		AsyncSupplier<Void> subscribe = coalesce(() -> Promise.complete().async());

		Promise<Void>[] nextPromise = new Promise[3];
		Promise<Void> promise1 = subscribe.get();

		Promise<Void> promise2 = subscribe.get();
		Promise<Void> promise3 = subscribe.get()
				.whenComplete(() -> nextPromise[0] = subscribe.get()
						.whenComplete(() -> nextPromise[1] = subscribe.get()
								.whenComplete(() -> nextPromise[2] = subscribe.get())));
		Promise<Void> promise4 = subscribe.get();

		await(promise1);

		assertNotSame(promise1, promise2);

		assertSame(promise2, promise3);
		assertSame(promise2, promise4);

		// first recursion subscribed to new promise
		assertNotSame(nextPromise[0], promise1);
		assertNotSame(nextPromise[0], promise2);

		// next recursions subscribed to newly created promises
		assertNotSame(nextPromise[0], nextPromise[1]);
		assertNotSame(nextPromise[1], nextPromise[2]);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void subscribeMultipleRecursions() {
		AsyncSupplier<Void> subscribe = coalesce(() -> Promise.complete().async());

		Promise<Void>[] nextPromise1 = new Promise[3];
		Promise<Void>[] nextPromise2 = new Promise[3];
		Promise<Void> promise1 = subscribe.get();

		Promise<Void> promise2 = subscribe.get();
		Promise<Void> promise3 = subscribe.get()
				.whenComplete(() -> nextPromise1[0] = subscribe.get()
						.whenComplete(() -> nextPromise1[1] = subscribe.get()
								.whenComplete(() -> nextPromise1[2] = subscribe.get())));
		Promise<Void> promise4 = subscribe.get()
				.whenComplete(() -> nextPromise2[0] = subscribe.get()
						.whenComplete(() -> nextPromise2[1] = subscribe.get()
								.whenComplete(() -> nextPromise2[2] = subscribe.get())));

		await(promise1);

		assertNotSame(promise1, promise2);

		assertSame(promise2, promise3);
		assertSame(promise2, promise4);

		// first recursions subscribed to new promise and are the same
		assertNotSame(nextPromise1[0], promise1);
		assertNotSame(nextPromise2[0], promise1);
		assertNotSame(nextPromise1[0], promise2);
		assertNotSame(nextPromise2[0], promise2);
//		assertSame(nextPromise1[0], nextPromise2[0]);

		// next recursions subscribed to newly created promises and are the same (between each other)
		assertNotSame(nextPromise1[0], nextPromise1[1]);
		assertNotSame(nextPromise1[1], nextPromise1[2]);

		assertNotSame(nextPromise2[0], nextPromise2[1]);
		assertNotSame(nextPromise2[1], nextPromise2[2]);

		assertSame(nextPromise1[1], nextPromise1[1]);
		assertSame(nextPromise1[2], nextPromise1[2]);
	}

	@Test
	public void subscribeIfNotAsync() {
		AsyncSupplier<Void> supplier = coalesce(Promise::complete);

		Ref<Promise<Void>> nextPromiseRef = new Ref<>();
		Promise<Void> promise1 = supplier.get();
		Promise<Void> promise2 = supplier.get()
				.whenComplete(() -> nextPromiseRef.value = supplier.get());
		Promise<Void> promise3 = supplier.get();

		await(promise1);

		assertNotSame(promise1, promise2);
		assertNotSame(promise1, promise3);
		assertNotSame(promise1, nextPromiseRef.value);
		assertNotSame(promise2, promise3);
		assertNotSame(promise2, nextPromiseRef.value);
		assertNotSame(promise3, nextPromiseRef.value);
	}
}
