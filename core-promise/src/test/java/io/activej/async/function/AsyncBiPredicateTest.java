package io.activej.async.function;

import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.*;

public class AsyncBiPredicateTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void predicateOf() {
		AsyncBiPredicate<Integer, Integer> predicate = AsyncBiPredicate.of((i1, i2) -> (i1 + i2) % 2 == 0);

		assertTrue(await(predicate.test(0, 0)));
		assertFalse(await(predicate.test(0, 1)));
		assertFalse(await(predicate.test(1, 0)));
		assertTrue(await(predicate.test(1, 1)));
		assertTrue(await(predicate.test(2, 0)));
		assertTrue(await(predicate.test(0, 2)));
	}

	@Test
	public void predicateThrows() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> predicate = AsyncBiPredicate.of((i1, i2) -> {
			throw expected;
		});

		Exception exception = awaitException(predicate.test(0, 0));
		assertSame(expected, exception);
	}

	@Test
	public void asyncPredicate() {
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> async((i1 + i2) % 2 == 0);

		assertTrue(await(predicate.test(0, 0)));
		assertFalse(await(predicate.test(0, 1)));
		assertFalse(await(predicate.test(1, 0)));
		assertTrue(await(predicate.test(1, 1)));
		assertTrue(await(predicate.test(2, 0)));
		assertTrue(await(predicate.test(0, 2)));
	}

	@Test
	public void asyncPredicateException() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> Promise.ofException(expected);

		Exception exception = awaitException(predicate.test(0, 0));
		assertSame(expected, exception);
	}

	@Test
	public void not() {
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> async((i1 + i2) % 2 == 0);
		AsyncBiPredicate<Integer, Integer> notPredicate = AsyncBiPredicate.not(predicate);

		assertTrue(await(predicate.test(0, 0)));
		assertFalse(await(predicate.test(0, 1)));
		assertFalse(await(predicate.test(1, 0)));
		assertTrue(await(predicate.test(1, 1)));
		assertTrue(await(predicate.test(2, 0)));
		assertTrue(await(predicate.test(0, 2)));

		assertFalse(await(notPredicate.test(0, 0)));
		assertTrue(await(notPredicate.test(0, 1)));
		assertTrue(await(notPredicate.test(1, 0)));
		assertFalse(await(notPredicate.test(1, 1)));
		assertFalse(await(notPredicate.test(2, 0)));
		assertFalse(await(notPredicate.test(0, 2)));
	}

	@Test
	public void notException() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> Promise.ofException(expected);
		AsyncBiPredicate<Integer, Integer> notPredicate = AsyncBiPredicate.not(predicate);

		Exception exception = awaitException(notPredicate.test(0, 0));
		assertSame(expected, exception);
	}

	@Test
	public void negate() {
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> async((i1 + i2) % 2 == 0);
		AsyncBiPredicate<Integer, Integer> notPredicate = predicate.negate();

		assertTrue(await(predicate.test(0, 0)));
		assertFalse(await(predicate.test(0, 1)));
		assertFalse(await(predicate.test(1, 0)));
		assertTrue(await(predicate.test(1, 1)));
		assertTrue(await(predicate.test(2, 0)));
		assertTrue(await(predicate.test(0, 2)));

		assertFalse(await(notPredicate.test(0, 0)));
		assertTrue(await(notPredicate.test(0, 1)));
		assertTrue(await(notPredicate.test(1, 0)));
		assertFalse(await(notPredicate.test(1, 1)));
		assertFalse(await(notPredicate.test(2, 0)));
		assertFalse(await(notPredicate.test(0, 2)));
	}

	@Test
	public void negateException() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> Promise.ofException(expected);
		AsyncBiPredicate<Integer, Integer> notPredicate = predicate.negate();

		Exception exception = awaitException(notPredicate.test(0, 0));
		assertSame(expected, exception);
	}

	@Test
	public void and() {
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3 = (i1, i2) -> async((i1 + i2) % 3 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf4 = (i1, i2) -> async((i1 + i2) % 4 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3And4 = isSumMultipleOf3.and(isSumMultipleOf4);

		int number1 = 1, number2 = 1;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4.test(number1, number2)));

		number1 = 2;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4.test(number1, number2)));

		number2 = 2;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4.test(number1, number2)));

		number1 = 10;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3And4.test(number1, number2)));
	}

	@Test
	public void andException() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> async((i1 + i2) % 2 == 0);
		AsyncBiPredicate<Integer, Integer> failingPredicate = (i1, i2) -> Promise.ofException(expected);
		AsyncBiPredicate<Integer, Integer> andPredicate = predicate.and(failingPredicate);

		Exception exception = awaitException(andPredicate.test(0, 0));
		assertSame(expected, exception);
	}

	@Test
	public void or() {
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3 = (i1, i2) -> async((i1 + i2) % 3 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf4 = (i1, i2) -> async((i1 + i2) % 4 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3Or4 = isSumMultipleOf3.or(isSumMultipleOf4);

		int number1 = 1, number2 = 1;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3Or4.test(number1, number2)));

		number1 = 2;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4.test(number1, number2)));

		number2 = 2;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4.test(number1, number2)));

		number1 = 10;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4.test(number1, number2)));
	}

	@Test
	public void orException() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> predicate = (i1, i2) -> async((i1 + i2) % 2 == 0);
		AsyncBiPredicate<Integer, Integer> failingPredicate = (i1, i2) -> Promise.ofException(expected);
		AsyncBiPredicate<Integer, Integer> orPredicate = predicate.or(failingPredicate);

		Exception exception = awaitException(orPredicate.test(0, 0));
		assertSame(expected, exception);
	}

	static Promise<Boolean> async(boolean value) {
		return Promise.complete().async()
				.map($ -> value);
	}
}
