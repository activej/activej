package io.activej.common;

import io.activej.common.ref.RefBoolean;
import org.junit.Test;

import java.util.stream.Collector;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class CollectorsExTest {
	@Test
	public void testToVoid() {
		Void collect = Stream.of(1, 2, 3).collect(CollectorsEx.toVoid());
		assertNull(collect);
	}

	@Test
	public void testToFirst() {
		Integer collect = Stream.of(1, 2, 3).collect(CollectorsEx.toFirst());
		assertEquals(1, collect.intValue());
	}

	@Test
	public void testToLast() {
		Integer collect = Stream.of(1, 2, 3).collect(CollectorsEx.toLast());
		assertEquals(3, collect.intValue());
	}

	@Test
	public void testToAll() {
		Collector<Boolean, RefBoolean, Boolean> collector = CollectorsEx.toAll();
		Boolean collect = Stream.of(true, true).collect(collector);
		assertTrue(collect);

		collect = Stream.of(true, false).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, true).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, false).collect(collector);
		assertFalse(collect);
	}

	@Test
	public void testToAllWithPredicate() {
		Boolean collect = Stream.of(2, 4).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(2, 5).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 4).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 5).collect(CollectorsEx.toAll(number -> number % 2 == 0));
		assertFalse(collect);
	}

	@Test
	public void testToAny() {
		Collector<Boolean, RefBoolean, Boolean> collector = CollectorsEx.toAny();

		Boolean collect = Stream.of(true, true).collect(collector);
		assertTrue(collect);

		collect = Stream.of(true, false).collect(collector);
		assertTrue(collect);

		collect = Stream.of(false, true).collect(collector);
		assertTrue(collect);

		collect = Stream.of(false, false).collect(collector);
		assertFalse(collect);
	}

	@Test
	public void testToAnyWithPredicate() {
		Boolean collect = Stream.of(2, 4).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(2, 5).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(3, 4).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertTrue(collect);

		collect = Stream.of(3, 5).collect(CollectorsEx.toAny(number -> number % 2 == 0));
		assertFalse(collect);
	}

	@Test
	public void testToNone() {
		Collector<Boolean, RefBoolean, Boolean> collector = CollectorsEx.toNone();

		Boolean collect = Stream.of(true, true).collect(collector);
		assertFalse(collect);

		collect = Stream.of(true, false).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, true).collect(collector);
		assertFalse(collect);

		collect = Stream.of(false, false).collect(collector);
		assertTrue(collect);
	}

	@Test
	public void testToNoneWithPredicate() {
		Boolean collect = Stream.of(2, 4).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(2, 5).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 4).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertFalse(collect);

		collect = Stream.of(3, 5).collect(CollectorsEx.toNone(number -> number % 2 == 0));
		assertTrue(collect);
	}

}
