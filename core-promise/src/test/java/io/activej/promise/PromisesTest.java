package io.activej.promise;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.collection.Try;
import io.activej.common.exception.StacklessException;
import io.activej.common.tuple.*;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.promise.Promises.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;

public final class PromisesTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private final AtomicInteger counter = new AtomicInteger();

	@Test
	public void toListEmptyTest() {
		List<Integer> list = await(Promises.toList());
		assertEquals(0, list.size());
		// asserting immutability
		try {
			list.add(123);
		} catch (UnsupportedOperationException e) {
			return;
		}
		fail();
	}

	@Test
	public void toListSingleTest() {
		List<Integer> list = await(Promises.toList(Promise.of(321)));
		assertEquals(1, list.size());
	}

	@Test
	public void varargsToListTest() {
		List<Integer> list = await(Promises.toList(Promise.of(321), Promise.of(322), Promise.of(323)));
		assertEquals(3, list.size());
	}

	@Test
	public void streamToListTest() {
		List<Integer> list = await(Promises.toList(Stream.of(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, list.size());
	}

	@Test
	public void listToListTest() {
		List<Integer> list = await(Promises.toList(asList(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, list.size());
	}

	@Test
	public void toArrayEmptyTest() {
		Object[] array = await(toArray(Object.class));
		assertEquals(0, array.length);
	}

	@Test
	public void toArraySingleTest() {
		Integer[] array = await(toArray(Integer.class, Promise.of(321)));
		assertEquals(1, array.length);
		assertEquals(new Integer(321), array[0]);

	}

	@Test
	public void arrayToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Promise.of(321), Promise.of(322)));
		assertEquals(2, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
	}

	@Test
	public void varargsToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Promise.of(321), Promise.of(322), Promise.of(323)));
		assertEquals(3, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
		assertEquals(new Integer(323), array[2]);

	}

	@Test
	public void streamToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Stream.of(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
		assertEquals(new Integer(323), array[2]);
	}

	@Test
	public void listToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, asList(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, array.length);
		assertEquals(new Integer(321), array[0]);
		assertEquals(new Integer(322), array[1]);
		assertEquals(new Integer(323), array[2]);
	}

	@Test
	public void toTuple1Test() {
		Tuple1<Integer> tuple1 = await(toTuple(Tuple1::new, Promise.of(321)));
		assertEquals(new Integer(321), tuple1.getValue1());

		Tuple1<Integer> tuple2 = await(toTuple(Promise.of(321)));
		assertEquals(new Integer(321), tuple2.getValue1());
	}

	@Test
	public void toTuple2Test() {
		Tuple2<Integer, String> tuple1 = await(toTuple(Tuple2::new, Promise.of(321), Promise.of("322")));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());

		Tuple2<Integer, String> tuple2 = await(toTuple(Promise.of(321), Promise.of("322")));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
	}

	@Test
	public void toTuple3Test() {
		Tuple3<Integer, String, Double> tuple1 = await(toTuple(Tuple3::new, Promise.of(321), Promise.of("322"), Promise.of(323.34)));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());

		Tuple3<Integer, String, Double> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34)));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
	}

	@Test
	public void toTuple4Test() {
		Tuple4<Integer, String, Double, Duration> tuple1 = await(toTuple(Tuple4::new, Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324))));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());
		assertEquals(ofMillis(324), tuple1.getValue4());

		Tuple4<Integer, String, Double, Duration> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324))));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
		assertEquals(ofMillis(324), tuple2.getValue4());
	}

	@Test
	public void toTuple5Test() {
		Tuple5<Integer, String, Double, Duration, Integer> tuple1 = await(toTuple(Tuple5::new, Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1)));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());
		assertEquals(ofMillis(324), tuple1.getValue4());
		assertEquals(new Integer(1), tuple1.getValue5());

		Tuple5<Integer, String, Double, Duration, Integer> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1)));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
		assertEquals(ofMillis(324), tuple2.getValue4());
		assertEquals(new Integer(1), tuple2.getValue5());
	}

	@Test
	public void toTuple6Test() {
		Tuple6<Integer, String, Double, Duration, Integer, Object> tuple1 = await(toTuple(Tuple6::new, Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1), Promise.of(null)));
		assertEquals(new Integer(321), tuple1.getValue1());
		assertEquals("322", tuple1.getValue2());
		assertEquals(323.34, tuple1.getValue3());
		assertEquals(ofMillis(324), tuple1.getValue4());
		assertEquals(new Integer(1), tuple1.getValue5());
		assertNull(tuple1.getValue6());

		Tuple6<Integer, String, Double, Duration, Integer, Object> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1), Promise.of(null)));
		assertEquals(new Integer(321), tuple2.getValue1());
		assertEquals("322", tuple2.getValue2());
		assertEquals(323.34, tuple2.getValue3());
		assertEquals(ofMillis(324), tuple2.getValue4());
		assertEquals(new Integer(1), tuple2.getValue5());
		assertNull(tuple2.getValue6());
	}

	@Test
	public void testCollectStream() {
		List<Integer> list = await(Promises.toList(Stream.of(Promise.of(1), Promise.of(2), Promise.of(3))));
		assertEquals(3, list.size());
	}

	@Test
	public void testRepeat() {
		Exception exception = new Exception();
		Throwable e = awaitException(repeat(() -> {
			if (counter.get() == 5) {
				return Promise.ofException(exception);
			}
			counter.incrementAndGet();
			return Promise.of(null);
		}));
		System.out.println(counter);
		assertSame(exception, e);
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoop() {
		Promises.loop(0,
				i -> i < 5,
				i -> Promise.of(i + 1)
						.whenResult(counter::set));
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoopAsync() {
		await(Promises.loop(0,
				i -> i < 5,
				i -> Promises.delay(10L, i + 1)
						.whenResult(counter::set)));
		assertEquals(5, counter.get());
	}

	@Test
	public void testRunSequence() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		await(sequence(list.stream()
				.map(n ->
						() -> getPromise(n).toVoid())));
	}

	@Test
	public void testRunSequenceWithSorted() {
		List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);
		await(sequence(list.stream()
				.sorted(Comparator.naturalOrder())
				.map(n ->
						() -> getPromise(n).toVoid())));
	}

	@Test
	public void testSomeMethodWithZeroParam() {
		StacklessException exception = awaitException(some(100));
		assertEquals("There are no promises to be complete", exception.getMessage());
	}

	@Test
	public void testSomeMethodWithOneParamAndGetOne() {
		Integer result = 100;
		assertEquals(singletonList(100), await(some(Promise.of(result), 1)));
	}

	@Test
	public void testSomeMethodWithOneParamAndGetNone() {
		Integer value = 100;
		List<Integer> list = await(some(Promise.of(value), 0));
		assertTrue(list.isEmpty());
	}

	@Test
	public void testSomeMethodWithTwoParamAndGetTwo() {
		int result1 = 100;
		int result2 = 101;
		List<Integer> list = await(some(Promise.of(result1), Promise.of(result2), 2));
		assertEquals(asList(result1, result2), list);
	}

	@Test
	public void testSomeMethodWithTwoParamAndGetOne() {
		Integer result = 100;
		List<Integer> list = await(some(Promise.of(result), Promise.of(result), 1));

		assertEquals(singletonList(100), list);
	}

	@Test
	public void testSomeWithManyParamsAndGetHalfOfThem() {
		List<Promise<Integer>> params = Stream.generate(() -> Promise.of(0)).limit(10).collect(Collectors.toList());
		List<Integer> list = await(some(params, params.size() / 2));
		assertEquals(params.size() / 2, list.size());
	}

	@Test
	public void testSomeWithManyParamsAndGetNone() {
		List<Promise<Integer>> params = Stream.generate(() -> Promise.of(0)).limit(10).collect(Collectors.toList());

		List<Integer> list = await(some(params, 0));
		assertTrue(list.isEmpty());
	}

	@Test
	public void testSomeWithManyParamsWithDelayAndGetHalfOfThem() {
		List<Promise<Integer>> params = Stream.generate(() -> delay(100L, 0)).limit(10)
				.collect(Collectors.toList());

		List<Integer> list = await(some(params, params.size() / 2));
		assertEquals(params.size() / 2, list.size());
	}

	@Test
	public void testSomeTheWholeAreFailed() {
		List<CompleteExceptionallyPromise<Object>> params = Stream.generate(() -> Promise.ofException(new RuntimeException()))
				.limit(10)
				.collect(Collectors.toList());

		Throwable exception = awaitException(some(params, params.size() / 2));
		assertEquals("There are not enough promises to be complete", exception.getMessage());
	}

	@Test
	public void testSomeNotEnoughCompleteResult() {
		List<Promise<?>> params = asList(Promise.of(10),
				delay(100L, Promise.ofException(new RuntimeException())),
				Promise.of(100),
				Promise.ofException(new RuntimeException()));

		StacklessException exception = awaitException(some(params, 3));
		assertEquals("There are not enough promises to be complete", exception.getMessage());
	}

	@Test
	public void allSettledHasExceptionTest() {
		Exception expectedException = new Exception("test");
		Promise<Integer> p1 = Promise.of(10);
		Promise<Integer> p2 = Promises.delay(2, Promise.ofException(expectedException));
		Promise<Integer> p3 = Promises.delay(5, 10);
		Promise<Integer> p4 = Promises.delay(10, 20);

		Promise<Void> settled = Promises.allCompleted(Stream.of(p1, p2, p3, p4).iterator());

		assertEquals(expectedException, awaitException(settled));
	}

	@Test
	public void allSettledHasSeveralExceptionsAndSaveOrderTest() {
		Exception expectedException1 = new Exception("test1");
		Exception expectedException2 = new Exception("test2");
		Promise<Integer> p1 = Promise.of(10);
		Promise<Integer> p2 = Promises.delay(2, Promise.ofException(expectedException1));
		Promise<Integer> p3 = Promises.delay(5, Promise.ofException(expectedException2));
		Promise<Integer> p4 = Promises.delay(10, 20);

		Promise<Void> settled = Promises.allCompleted(Stream.of(p1, p2, p3, p4).iterator());

		assertEquals(expectedException2, awaitException(settled));
	}

	@Test
	public void allSettledHasSeveralExceptionsInSameTimeTest() {
		Exception expectedException1 = new Exception("test1");
		Exception expectedException2 = new Exception("test2");
		Promise<Integer> p1 = Promise.of(10);
		Promise<Integer> p2 = Promises.delay(5, Promise.ofException(expectedException1));
		Promise<Integer> p3 = Promises.delay(5, Promise.ofException(expectedException2));
		Promise<Integer> p4 = Promises.delay(10, 20);

		Promise<Void> settled = Promises.allCompleted(Stream.of(p1, p2, p3, p4).iterator());
		assertEquals(expectedException2, awaitException(settled));
	}

	@Test
	public void allWithCompletingIterator() {
		Exception e = new Exception();

		// success when all succeed
		doTestCompletingIterator(cb -> cb.set(null), Promise::complete, it -> await(Promises.all(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.complete().async(), it -> await(Promises.all(it)));

		// fail on single failed
		doTestCompletingIterator(cb -> cb.setException(e), Promise::complete, it -> assertSame(e, awaitException(Promises.all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.complete().async(), it -> assertSame(e, awaitException(Promises.all(it))));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e), it -> assertSame(e, awaitException(Promises.all(it))));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e).async(), it -> assertSame(e, awaitException(Promises.all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e), it -> assertSame(e, awaitException(Promises.all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e).async(), it -> assertSame(e, awaitException(Promises.all(it))));
	}

	@Test
	public void anyWithCompletingIterator() {
		Exception e = new Exception();

		// success when any succeed
		doTestCompletingIterator(cb -> cb.set(null), Promise::complete, it -> await(Promises.any(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.complete().async(), it -> await(Promises.any(it)));
		doTestCompletingIterator(cb -> cb.setException(e), Promise::complete, it -> await(Promises.any(it)));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.complete().async(), it -> await(Promises.any(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e), it -> await(Promises.any(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e).async(), it -> await(Promises.any(it)));

		// fail when all failed
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e), it -> assertSame(e, awaitException(Promises.all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e).async(), it -> assertSame(e, awaitException(Promises.all(it))));
	}

	@Test
	public void someWithCompletingIterator() {
		Exception e = new Exception();

		// testing for some(it, 1)
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2), it -> assertEquals(singletonList(1), await(Promises.some(it, 1))));
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2).async(), it -> assertEquals(singletonList(1), await(Promises.some(it, 1))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2), it -> assertEquals(singletonList(2), await(Promises.some(it, 1))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2).async(), it -> assertEquals(singletonList(2), await(Promises.some(it, 1))));
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.ofException(e), it -> assertEquals(singletonList(1), await(Promises.some(it, 1))));
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.ofException(e).async(), it -> assertEquals(singletonList(1), await(Promises.some(it, 1))));

		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 1)).getMessage()));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e).async(),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 1)).getMessage()));

		// testing for some(it, 2)
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2), it -> assertEquals(asList(1, 2), await(Promises.some(it, 2))));
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2).async(), it -> assertEquals(asList(1, 2), await(Promises.some(it, 2))));

		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 2)).getMessage()));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2).async(),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 2)).getMessage()));
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.ofException(e),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 2)).getMessage()));
		doTestCompletingIterator(cb -> cb.set(1), () -> Promise.ofException(e).async(),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 2)).getMessage()));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 2)).getMessage()));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e).async(),
				it -> assertEquals("There are not enough promises to be complete", awaitException(Promises.some(it, 2)).getMessage()));
	}

	@Test
	public void reduceWithLazyIterator() {
		Exception e = new Exception();

		for (int maxCalls = 1; maxCalls < 4; maxCalls++) {
			int finalMaxCalls = maxCalls;
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2), it ->
					assertEquals(asList(1, 2), await(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2).async(), it ->
					assertEquals(asList(1, 2), await(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2), it ->
					assertSame(e, awaitException(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2).async(), it ->
					assertSame(e, awaitException(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.<Integer>ofException(e), it ->
					assertSame(e, awaitException(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.<Integer>ofException(e).async(), it ->
					assertSame(e, awaitException(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.<Integer>ofException(e), it ->
					assertSame(e, awaitException(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.<Integer>ofException(e).async(), it ->
					assertSame(e, awaitException(reduce(it, finalMaxCalls, new ArrayList<Integer>(), ArrayList::add, o -> o))));
		}
	}

	@Test
	public void reduceExWithLazyIterator() {
		Exception e = new Exception();
		BiFunction<List<Integer>, Try<Integer>, Try<List<Integer>>> consumer = new BiFunction<List<Integer>, Try<Integer>, Try<List<Integer>>>() {
			int calls = 2;
			@Override
			public Try<List<Integer>> apply(List<Integer> integers, Try<Integer> integerTry) {
				if (integerTry.isSuccess()) {
					integers.add(integerTry.get());
				}
				if (--calls == 0) {
					calls = 2;
					return Try.of(integers);
				}
				return null;
			}
		};

		for (int maxCalls = 1; maxCalls < 4; maxCalls++) {
			int finalMaxCalls = maxCalls;
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2), it ->
					assertEquals(asList(1, 2), await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {}))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2).async(), it ->
					assertEquals(asList(1, 2), await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {}))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2), it ->
					assertEquals(singletonList(2), await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {}))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2).async(), it ->
					assertEquals(singletonList(2), await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {}))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.<Integer>ofException(e), it ->
					assertEquals(singletonList(1), await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {}))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.<Integer>ofException(e).async(), it ->
					assertEquals(singletonList(1), await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {}))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.<Integer>ofException(e), it ->
					assertTrue(await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {})).isEmpty()));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.<Integer>ofException(e).async(), it ->
					assertTrue(await(reduceEx(it, $ -> finalMaxCalls, new ArrayList<>(), consumer, Try::of, $ -> {})).isEmpty()));
		}
	}

	@Test
	public void testFirstSuccessfulForStackOverflow() {
		Exception exception = new Exception();
		Stream<AsyncSupplier<?>> suppliers = Stream.concat(
				Stream.generate(() -> AsyncSupplier.cast(() -> Promise.ofException(exception))).limit(100_000),
				Stream.of(AsyncSupplier.cast(Promise::complete))
		);
		await(Promises.firstSuccessful(suppliers));
	}

	@Test
	public void testUntilForStackOverflow() {
		await(until(0, number -> Promise.of(++number), number -> number == 100_000));
	}

	// For testing cases when a some previous promise in Iterator is being completed by calling Iterator::hasNext or Iterator::next
	private static <T> void doTestCompletingIterator(Consumer<SettablePromise<T>> firstPromiseConsumer, AsyncSupplier<T> secondPromiseSupplier,
			Consumer<Iterator<Promise<T>>> assertion) {

		// completion inside stream
		SettablePromise<T> settablePromise = new SettablePromise<>();
		Iterator<Promise<T>> iteratorOfStream = Stream.of(1, 2)
				.map(count -> {
					if (count == 1) {
						return settablePromise;
					} else {
						firstPromiseConsumer.accept(settablePromise);
						return secondPromiseSupplier.get();
					}
				}).iterator();

		// completion inside Iterator::next
		Iterator<Promise<T>> iteratorNext = new Iterator<Promise<T>>() {
			SettablePromise<T> settablePromise;

			@Override
			public boolean hasNext() {
				return settablePromise == null || !settablePromise.isComplete();
			}

			@Override
			public Promise<T> next() {
				if (settablePromise == null) {
					settablePromise = new SettablePromise<>();
					return settablePromise;
				} else {
					firstPromiseConsumer.accept(settablePromise);
					return secondPromiseSupplier.get();
				}
			}
		};

		// completion inside Iterator::hasNext
		Iterator<Promise<T>> iteratorHasNext = new Iterator<Promise<T>>() {
			SettablePromise<T> settablePromise;

			@Override
			public boolean hasNext() {
				if (settablePromise == null) {
					return true;
				} else if (!settablePromise.isComplete()) {
					firstPromiseConsumer.accept(settablePromise);
					return true;
				} else {
					return false;
				}
			}

			@Override
			public Promise<T> next() {
				if (settablePromise == null) {
					settablePromise = new SettablePromise<>();
					return settablePromise;
				} else {
					return secondPromiseSupplier.get();
				}
			}
		};

		assertion.accept(iteratorOfStream);
		assertion.accept(iteratorNext);
		assertion.accept(iteratorHasNext);
	}

	private Promise<Integer> getPromise(Integer number) {
		assertEquals(0, counter.get());
		counter.incrementAndGet();
		SettablePromise<Integer> promise = new SettablePromise<>();
		Eventloop.getCurrentEventloop().post(() -> promise.set(number));
		return promise
				.then(n -> {
					counter.decrementAndGet();
					return Promise.of(n);
				});
	}
}
