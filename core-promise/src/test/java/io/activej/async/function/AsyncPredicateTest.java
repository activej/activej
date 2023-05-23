package io.activej.async.function;

import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.*;

public class AsyncPredicateTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void predicateOf() {
		AsyncPredicate<Integer> predicate = AsyncPredicate.of(i -> i % 2 == 0);

		assertTrue(await(predicate.test(0)));
		assertFalse(await(predicate.test(1)));
	}

	@Test
	public void predicateThrows() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> predicate = AsyncPredicate.of(i -> {
			throw expected;
		});

		Exception exception = awaitException(predicate.test(0));
		assertSame(expected, exception);
	}

	@Test
	public void predicate() {
		AsyncPredicate<Integer> predicate = i -> async(i % 2 == 0);

		assertTrue(await(predicate.test(0)));
		assertFalse(await(predicate.test(1)));
	}

	@Test
	public void predicateException() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> predicate = i -> Promise.ofException(expected);

		Exception exception = awaitException(predicate.test(0));
		assertSame(expected, exception);
	}

	@Test
	public void not() {
		AsyncPredicate<Integer> predicate = i -> async(i % 2 == 0);
		AsyncPredicate<Integer> notPredicate = AsyncPredicate.not(predicate);

		assertTrue(await(predicate.test(0)));
		assertFalse(await(predicate.test(1)));

		assertFalse(await(notPredicate.test(0)));
		assertTrue(await(notPredicate.test(1)));
	}

	@Test
	public void notException() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> predicate = i -> Promise.ofException(expected);
		AsyncPredicate<Integer> notPredicate = AsyncPredicate.not(predicate);

		Exception exception = awaitException(notPredicate.test(0));
		assertSame(expected, exception);
	}

	@Test
	public void negate() {
		AsyncPredicate<Integer> predicate = i -> async(i % 2 == 0);
		AsyncPredicate<Integer> notPredicate = predicate.negate();

		assertTrue(await(predicate.test(0)));
		assertFalse(await(predicate.test(1)));

		assertFalse(await(notPredicate.test(0)));
		assertTrue(await(notPredicate.test(1)));
	}

	@Test
	public void negateException() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> predicate = i -> Promise.ofException(expected);
		AsyncPredicate<Integer> notPredicate = predicate.negate();

		Exception exception = awaitException(notPredicate.test(0));
		assertSame(expected, exception);
	}

	@Test
	public void and() {
		AsyncPredicate<Integer> isMultipleOf3 = i -> async(i % 3 == 0);
		AsyncPredicate<Integer> isMultipleOf4 = i -> async(i % 4 == 0);
		AsyncPredicate<Integer> isMultipleOf3And4 = isMultipleOf3.and(isMultipleOf4);

		int number = 1;
		assertFalse(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf3And4.test(number)));

		number = 3;
		assertTrue(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf3And4.test(number)));

		number = 4;
		assertFalse(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf3And4.test(number)));

		number = 12;
		assertTrue(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf3And4.test(number)));
	}

	@Test
	public void andException() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> predicate = i -> async(i % 2 == 0);
		AsyncPredicate<Integer> failingPredicate = i -> Promise.ofException(expected);
		AsyncPredicate<Integer> andPredicate = predicate.and(failingPredicate);

		Exception exception = awaitException(andPredicate.test(0));
		assertSame(expected, exception);
	}

	@Test
	public void or() {
		AsyncPredicate<Integer> isMultipleOf3 = i -> async(i % 3 == 0);
		AsyncPredicate<Integer> isMultipleOf4 = i -> async(i % 4 == 0);
		AsyncPredicate<Integer> isMultipleOf3Or4 = isMultipleOf3.or(isMultipleOf4);

		int number = 1;
		assertFalse(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf3Or4.test(number)));

		number = 3;
		assertTrue(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf3Or4.test(number)));

		number = 4;
		assertFalse(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf3Or4.test(number)));

		number = 12;
		assertTrue(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf3Or4.test(number)));
	}

	@Test
	public void orException() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> predicate = i -> async(i % 2 == 0);
		AsyncPredicate<Integer> failingPredicate = i -> Promise.ofException(expected);
		AsyncPredicate<Integer> orPredicate = predicate.or(failingPredicate);

		Exception exception = awaitException(orPredicate.test(0));
		assertSame(expected, exception);
	}

	static Promise<Boolean> async(boolean value) {
		return Promise.complete().async()
			.map($ -> value);
	}
}
