package io.activej.promise;

import io.activej.common.ref.RefInt;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class AbstractPromiseTest {

	@Test
	public void testRecursionProblems() {
		int count = 1_000_000;
		SettablePromise<Void> settablePromise = new SettablePromise<>();
		RefInt refInt = new RefInt(count);
		for (int i = 0; i < count; i++) {
			settablePromise.whenResult(refInt::dec);
		}

		settablePromise.set(null);

		assertEquals(0, refInt.get());
	}

	@Test
	public void testOrder() {
		SettablePromise<@Nullable Void> settablePromise = new SettablePromise<>();
		StringBuilder sb = new StringBuilder();
		settablePromise
				.whenResult(() -> sb.append("1"))
				.whenResult(() -> sb.append("2"))
				.whenResult(() -> sb.append("3"))
				.whenResult(() -> sb.append("4"))
				.whenResult(() -> sb.append("5"));

		settablePromise.set(null);

		assertEquals("12345", sb.toString());

	}
}
