package io.activej.async.function;

import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.async.function.AsyncPredicateTest.async;
import static io.activej.async.function.AsyncPredicates.and;
import static io.activej.async.function.AsyncPredicates.or;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.*;

public class AsyncPredicatesTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testAnd() {
		AsyncPredicate<Integer> isMultipleOf3 = i -> async(i % 3 == 0);
		AsyncPredicate<Integer> isMultipleOf4 = i -> async(i % 4 == 0);
		AsyncPredicate<Integer> isMultipleOf10 = i -> async(i % 10 == 0);

		AsyncPredicate<Integer> isMultipleOf3And4And10 = and(isMultipleOf3, isMultipleOf4, isMultipleOf10);

		int number = 1;
		assertFalse(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertFalse(await(isMultipleOf3And4And10.test(number)));

		number = 3;
		assertTrue(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertFalse(await(isMultipleOf3And4And10.test(number)));

		number = 4;
		assertFalse(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertFalse(await(isMultipleOf3And4And10.test(number)));

		number = 10;
		assertFalse(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf10.test(number)));
		assertFalse(await(isMultipleOf3And4And10.test(number)));

		number = 12;
		assertTrue(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertFalse(await(isMultipleOf3And4And10.test(number)));

		number = 60;
		assertTrue(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf10.test(number)));
		assertTrue(await(isMultipleOf3And4And10.test(number)));
	}

	@Test
	public void testAndWithFailing() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> isMultipleOf3 = i -> async(i % 3 == 0);
		AsyncPredicate<Integer> isMultipleOf4 = i -> async(i % 4 == 0);
		AsyncPredicate<Integer> failingPredicate = i -> Promise.ofException(expected);

		AsyncPredicate<Integer> andPredicate = and(isMultipleOf3, isMultipleOf4, failingPredicate);

		Exception exception = awaitException(andPredicate.test(0));
		assertSame(exception, exception);
	}

	@Test
	public void testOr() {
		AsyncPredicate<Integer> isMultipleOf3 = i -> async(i % 3 == 0);
		AsyncPredicate<Integer> isMultipleOf4 = i -> async(i % 4 == 0);
		AsyncPredicate<Integer> isMultipleOf10 = i -> async(i % 10 == 0);

		AsyncPredicate<Integer> isMultipleOf3Or4Or10 = or(isMultipleOf3, isMultipleOf4, isMultipleOf10);

		int number = 1;
		assertFalse(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertFalse(await(isMultipleOf3Or4Or10.test(number)));

		number = 3;
		assertTrue(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertTrue(await(isMultipleOf3Or4Or10.test(number)));

		number = 4;
		assertFalse(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertTrue(await(isMultipleOf3Or4Or10.test(number)));

		number = 10;
		assertFalse(await(isMultipleOf3.test(number)));
		assertFalse(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf10.test(number)));
		assertTrue(await(isMultipleOf3Or4Or10.test(number)));

		number = 12;
		assertTrue(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertFalse(await(isMultipleOf10.test(number)));
		assertTrue(await(isMultipleOf3Or4Or10.test(number)));

		number = 60;
		assertTrue(await(isMultipleOf3.test(number)));
		assertTrue(await(isMultipleOf4.test(number)));
		assertTrue(await(isMultipleOf10.test(number)));
		assertTrue(await(isMultipleOf3Or4Or10.test(number)));
	}

	@Test
	public void testOrWithFailing() {
		ExpectedException expected = new ExpectedException();
		AsyncPredicate<Integer> isMultipleOf3 = i -> async(i % 3 == 0);
		AsyncPredicate<Integer> isMultipleOf4 = i -> async(i % 4 == 0);
		AsyncPredicate<Integer> failingPredicate = i -> Promise.ofException(expected);

		AsyncPredicate<Integer> andPredicate = or(isMultipleOf3, isMultipleOf4, failingPredicate);

		Exception exception = awaitException(andPredicate.test(0));
		assertSame(exception, exception);
	}
}
