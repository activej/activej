package io.activej.promise;

import io.activej.async.function.AsyncSupplier;
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
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.activej.promise.Promises.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.time.Duration.ofMillis;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@SuppressWarnings({"Convert2MethodRef"})
public final class PromisesTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private final AtomicInteger counter = new AtomicInteger();

	@Test
	public void toListEmptyTest() {
		List<Integer> list = await(toList());
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
		List<Integer> list = await(toList(Promise.of(321)));
		assertEquals(1, list.size());
	}

	@Test
	public void varargsToListTest() {
		List<Integer> list = await(toList(Promise.of(321), Promise.of(322), Promise.of(323)));
		assertEquals(3, list.size());
	}

	@Test
	public void streamToListTest() {
		List<Integer> list = await(toList(Stream.of(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, list.size());
	}

	@Test
	public void listToListTest() {
		List<Integer> list = await(toList(List.of(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, list.size());
	}

	@Test
	public void toListPreservesOrder() {
		List<Integer> list = await(toList(List.of(
				delay(20)
						.map($ -> 1)
						.whenResult(() -> System.out.println("First promise finished")),
				delay(10)
						.map($ -> 2)
						.whenResult(() -> System.out.println("Second promise finished")),
				Promise.of(3)
						.whenResult(() -> System.out.println("Third promise finished")),
				delay(30)
						.map($ -> 3)
						.whenResult(() -> System.out.println("Fourth promise finished")))));

		assertEquals(4, list.size());
		for (int i = 0; i < 3; i++) {
			assertEquals(Integer.valueOf(i + 1), list.get(i));
		}
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
		assertEquals(Integer.valueOf(321), array[0]);

	}

	@Test
	public void arrayToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Promise.of(321), Promise.of(322)));
		assertEquals(2, array.length);
		assertEquals(Integer.valueOf(321), array[0]);
		assertEquals(Integer.valueOf(322), array[1]);
	}

	@Test
	public void varargsToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Promise.of(321), Promise.of(322), Promise.of(323)));
		assertEquals(3, array.length);
		assertEquals(Integer.valueOf(321), array[0]);
		assertEquals(Integer.valueOf(322), array[1]);
		assertEquals(Integer.valueOf(323), array[2]);

	}

	@Test
	public void streamToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, Stream.of(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, array.length);
		assertEquals(Integer.valueOf(321), array[0]);
		assertEquals(Integer.valueOf(322), array[1]);
		assertEquals(Integer.valueOf(323), array[2]);
	}

	@Test
	public void listToArrayDoubleTest() {
		Integer[] array = await(toArray(Integer.class, List.of(Promise.of(321), Promise.of(322), Promise.of(323))));
		assertEquals(3, array.length);
		assertEquals(Integer.valueOf(321), array[0]);
		assertEquals(Integer.valueOf(322), array[1]);
		assertEquals(Integer.valueOf(323), array[2]);
	}

	@Test
	public void toTuple1Test() {
		Tuple1<Integer> tuple1 = await(toTuple(Tuple1::new, Promise.of(321)));
		assertEquals(Integer.valueOf(321), tuple1.value1());

		Tuple1<Integer> tuple2 = await(toTuple(Promise.of(321)));
		assertEquals(Integer.valueOf(321), tuple2.value1());
	}

	@Test
	public void toTuple2Test() {
		Tuple2<Integer, String> tuple1 = await(toTuple(Tuple2::new, Promise.of(321), Promise.of("322")));
		assertEquals(Integer.valueOf(321), tuple1.value1());
		assertEquals("322", tuple1.value2());

		Tuple2<Integer, String> tuple2 = await(toTuple(Promise.of(321), Promise.of("322")));
		assertEquals(Integer.valueOf(321), tuple2.value1());
		assertEquals("322", tuple2.value2());
	}

	@Test
	public void toTuple3Test() {
		Tuple3<Integer, String, Double> tuple1 = await(toTuple(Tuple3::new, Promise.of(321), Promise.of("322"), Promise.of(323.34)));
		assertEquals(Integer.valueOf(321), tuple1.value1());
		assertEquals("322", tuple1.value2());
		assertEquals(323.34, tuple1.value3());

		Tuple3<Integer, String, Double> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34)));
		assertEquals(Integer.valueOf(321), tuple2.value1());
		assertEquals("322", tuple2.value2());
		assertEquals(323.34, tuple2.value3());
	}

	@Test
	public void toTuple4Test() {
		Tuple4<Integer, String, Double, Duration> tuple1 = await(toTuple(Tuple4::new, Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324))));
		assertEquals(Integer.valueOf(321), tuple1.value1());
		assertEquals("322", tuple1.value2());
		assertEquals(323.34, tuple1.value3());
		assertEquals(ofMillis(324), tuple1.value4());

		Tuple4<Integer, String, Double, Duration> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324))));
		assertEquals(Integer.valueOf(321), tuple2.value1());
		assertEquals("322", tuple2.value2());
		assertEquals(323.34, tuple2.value3());
		assertEquals(ofMillis(324), tuple2.value4());
	}

	@Test
	public void toTuple5Test() {
		Tuple5<Integer, String, Double, Duration, Integer> tuple1 = await(toTuple(Tuple5::new, Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1)));
		assertEquals(Integer.valueOf(321), tuple1.value1());
		assertEquals("322", tuple1.value2());
		assertEquals(323.34, tuple1.value3());
		assertEquals(ofMillis(324), tuple1.value4());
		assertEquals(Integer.valueOf(1), tuple1.value5());

		Tuple5<Integer, String, Double, Duration, Integer> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1)));
		assertEquals(Integer.valueOf(321), tuple2.value1());
		assertEquals("322", tuple2.value2());
		assertEquals(323.34, tuple2.value3());
		assertEquals(ofMillis(324), tuple2.value4());
		assertEquals(Integer.valueOf(1), tuple2.value5());
	}

	@Test
	public void toTuple6Test() {
		Tuple6<Integer, String, Double, Duration, Integer, Object> tuple1 = await(toTuple(Tuple6::new, Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1), Promise.of(null)));
		assertEquals(Integer.valueOf(321), tuple1.value1());
		assertEquals("322", tuple1.value2());
		assertEquals(323.34, tuple1.value3());
		assertEquals(ofMillis(324), tuple1.value4());
		assertEquals(Integer.valueOf(1), tuple1.value5());
		assertNull(tuple1.value6());

		Tuple6<Integer, String, Double, Duration, Integer, Object> tuple2 = await(toTuple(Promise.of(321), Promise.of("322"), Promise.of(323.34), Promise.of(ofMillis(324)), Promise.of(1), Promise.of(null)));
		assertEquals(Integer.valueOf(321), tuple2.value1());
		assertEquals("322", tuple2.value2());
		assertEquals(323.34, tuple2.value3());
		assertEquals(ofMillis(324), tuple2.value4());
		assertEquals(Integer.valueOf(1), tuple2.value5());
		assertNull(tuple2.value6());
	}

	@Test
	public void testCollectStream() {
		List<Integer> list = await(toList(Stream.of(Promise.of(1), Promise.of(2), Promise.of(3))));
		assertEquals(3, list.size());
	}

	@Test
	public void testRepeat() {
		Exception exception = new Exception();
		Exception e = awaitException(repeat(() -> {
			if (counter.get() == 5) {
				return Promise.ofException(exception);
			}
			counter.incrementAndGet();
			return Promise.of(true);
		}));
		System.out.println(counter);
		assertSame(exception, e);
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoop() {
		loop(0,
				i -> i < 5,
				i -> Promise.of(i + 1)
						.whenResult(counter::set));
		assertEquals(5, counter.get());
	}

	@Test
	public void testLoopAsync() {
		await(loop(0,
				i -> i < 5,
				i -> delay(10L, i + 1)
						.whenResult(counter::set)));
		assertEquals(5, counter.get());
	}

	@Test
	public void testRunSequence() {
		List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);
		await(sequence(list.stream()
				.map(n ->
						() -> getPromise(n).toVoid())));
	}

	@Test
	public void testRunSequenceWithSorted() {
		List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7);
		await(sequence(list.stream()
				.sorted(Comparator.naturalOrder())
				.map(n ->
						() -> getPromise(n).toVoid())));
	}

	@Test
	public void allWithCompletingIterator() {
		Exception e = new Exception();

		// success when all succeed
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.complete(), it -> await(all(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.complete().async(), it -> await(all(it)));

		// fail on single failed
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.complete(), it -> assertSame(e, awaitException(all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.complete().async(), it -> assertSame(e, awaitException(all(it))));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e), it -> assertSame(e, awaitException(all(it))));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e).async(), it -> assertSame(e, awaitException(all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e), it -> assertSame(e, awaitException(all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e).async(), it -> assertSame(e, awaitException(all(it))));
	}

	@Test
	public void anyWithCompletingIterator() {
		Exception e = new Exception();

		// success when any succeed
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.complete(), it -> await(any(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.complete().async(), it -> await(any(it)));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.complete(), it -> await(any(it)));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.complete().async(), it -> await(any(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e), it -> await(any(it)));
		doTestCompletingIterator(cb -> cb.set(null), () -> Promise.ofException(e).async(), it -> await(any(it)));

		// fail when all failed
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e), it -> assertSame(e, awaitException(all(it))));
		doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.ofException(e).async(), it -> assertSame(e, awaitException(all(it))));
	}

	@Test
	public void reduceWithLazyIterator() {
		Exception e = new Exception();

		for (int maxCalls = 2; maxCalls < 5; maxCalls++) {
			int finalMaxCalls = maxCalls;
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2), it ->
					assertEquals(List.of(1, 2), await(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.of(2).async(), it ->
					assertEquals(List.of(1, 2), await(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2), it ->
					assertSame(e, awaitException(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.of(2).async(), it ->
					assertSame(e, awaitException(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.<Integer>ofException(e), it ->
					assertSame(e, awaitException(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.set(1), () -> Promise.<Integer>ofException(e).async(), it ->
					assertSame(e, awaitException(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.<Integer>ofException(e), it ->
					assertSame(e, awaitException(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
			doTestCompletingIterator(cb -> cb.setException(e), () -> Promise.<Integer>ofException(e).async(), it ->
					assertSame(e, awaitException(reduce(new ArrayList<Integer>(), ArrayList::add, o -> o, finalMaxCalls, it))));
		}
	}

	@Test
	public void testFirstSuccessfulForStackOverflow() {
		Exception exception = new Exception();
		Stream<AsyncSupplier<Void>> suppliers = Stream.concat(
				Stream.generate(() -> AsyncSupplier.<Void>of(() -> {
					throw exception;
				})).limit(100_000),
				Stream.of(AsyncSupplier.of(() -> null))
		);
		await(first(suppliers));
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
		Iterator<Promise<T>> iteratorNext = new Iterator<>() {
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

		assertion.accept(iteratorOfStream);
		assertion.accept(iteratorNext);
		// assertion.accept(iteratorHasNext);
	}

	private Promise<Integer> getPromise(Integer number) {
		assertEquals(0, counter.get());
		counter.incrementAndGet();
		SettablePromise<Integer> promise = new SettablePromise<>();
		Eventloop.getCurrentEventloop().post(() ->
				promise.set(number));
		return promise
				.whenResult(() ->
						counter.decrementAndGet());
	}
}
