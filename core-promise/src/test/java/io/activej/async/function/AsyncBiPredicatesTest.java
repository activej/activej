package io.activej.async.function;

import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.async.function.AsyncBiPredicates.and;
import static io.activej.async.function.AsyncBiPredicates.or;
import static io.activej.async.function.AsyncPredicateTest.async;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.*;

public class AsyncBiPredicatesTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testAnd() {
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3 = (i1, i2) -> async((i1 + i2) % 3 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf4 = (i1, i2) -> async((i1 + i2) % 4 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf10 = (i1, i2) -> async((i1 + i2) % 10 == 0);

		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3And4And10 = and(isSumMultipleOf3, isSumMultipleOf4, isSumMultipleOf10);

		int number1 = 1, number2 = 1;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4And10.test(number1, number2)));

		number1 = 2;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4And10.test(number1, number2)));

		number2 = 2;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4And10.test(number1, number2)));

		number1 = 8;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf10.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4And10.test(number1, number2)));

		number2 = 4;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3And4And10.test(number1, number2)));

		number1 = 56;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf10.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3And4And10.test(number1, number2)));
	}

	@Test
	public void testAndWithFailing() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3 = (i1, i2) -> async((i1 + i2) % 3 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf4 = (i1, i2) -> async((i1 + i2) % 4 == 0);
		AsyncBiPredicate<Integer, Integer> failingPredicate = (i1, i2) -> Promise.ofException(expected);

		AsyncBiPredicate<Integer, Integer> andPredicate = and(isSumMultipleOf3, isSumMultipleOf4, failingPredicate);

		Exception exception = awaitException(andPredicate.test(0, 0));
		assertSame(expected, exception);
	}

	@Test
	public void testOr() {
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3 = (i1, i2) -> async((i1 + i2) % 3 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf4 = (i1, i2) -> async((i1 + i2) % 4 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf10 = (i1, i2) -> async((i1 + i2) % 10 == 0);

		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3Or4Or10 = or(isSumMultipleOf3, isSumMultipleOf4, isSumMultipleOf10);

		int number1 = 1, number2 = 1;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertFalse(await(isSumMultipleOf3Or4Or10.test(number1, number2)));

		number1 = 2;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4Or10.test(number1, number2)));

		number2 = 2;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4Or10.test(number1, number2)));

		number1 = 8;
		assertFalse(await(isSumMultipleOf3.test(number1, number2)));
		assertFalse(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf10.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4Or10.test(number1, number2)));

		number2 = 4;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertFalse(await(isSumMultipleOf10.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4Or10.test(number1, number2)));

		number1 = 56;
		assertTrue(await(isSumMultipleOf3.test(number1, number2)));
		assertTrue(await(isSumMultipleOf4.test(number1, number2)));
		assertTrue(await(isSumMultipleOf10.test(number1, number2)));
		assertTrue(await(isSumMultipleOf3Or4Or10.test(number1, number2)));
	}

	@Test
	public void testOrWithFailing() {
		ExpectedException expected = new ExpectedException();
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf3 = (i1, i2) -> async((i1 + i2) % 3 == 0);
		AsyncBiPredicate<Integer, Integer> isSumMultipleOf4 = (i1, i2) -> async((i1 + i2) % 4 == 0);
		AsyncBiPredicate<Integer, Integer> failingPredicate = (i1, i2) -> Promise.ofException(expected);

		AsyncBiPredicate<Integer, Integer> andPredicate = or(isSumMultipleOf3, isSumMultipleOf4, failingPredicate);

		Exception exception = awaitException(andPredicate.test(0, 0));
		assertSame(expected, exception);
	}
}
