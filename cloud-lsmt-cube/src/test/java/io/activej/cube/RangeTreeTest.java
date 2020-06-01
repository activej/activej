package io.activej.cube;

import io.activej.aggregation.RangeTree;
import org.junit.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeTreeTest {

	@Test
	public void testPut() {
		RangeTree<Integer, String> rangeTree = RangeTree.create();
		rangeTree.put(1, 10, "a");
		rangeTree.put(3, 8, "b");
		rangeTree.put(5, 5, "c");
		rangeTree = RangeTree.cloneOf(rangeTree);
		assertEquals(Stream.of().collect(toSet()), rangeTree.get(0));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.get(1));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.get(2));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.get(3));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.get(4));
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), rangeTree.get(5));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.get(6));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.get(8));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.get(9));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.get(10));
		assertEquals(Stream.of().collect(toSet()), rangeTree.get(11));

		assertEquals(Stream.of().collect(toSet()), rangeTree.getRange(0, 0));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(1, 1));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(2, 2));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(3, 3));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(4, 4));
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), rangeTree.getRange(5, 5));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(6, 6));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(8, 8));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(9, 9));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(10, 10));
		assertEquals(Stream.of().collect(toSet()), rangeTree.getRange(11, 11));

		assertEquals(Stream.of().collect(toSet()), rangeTree.getRange(0, 0));
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), rangeTree.getRange(0, 11));

		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(1, 1));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(1, 2));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(1, 3));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(1, 4));
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), rangeTree.getRange(1, 5));

		assertEquals(Stream.of("a", "b", "c").collect(toSet()), rangeTree.getRange(5, 11));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(6, 11));
		assertEquals(Stream.of("a", "b").collect(toSet()), rangeTree.getRange(8, 11));
		assertEquals(Stream.of("a").collect(toSet()), rangeTree.getRange(9, 11));
		assertEquals(Stream.of().collect(toSet()), rangeTree.getRange(11, 11));
	}

	@Test
	public void testRemove() {
		RangeTree<Integer, String> rangeTree = RangeTree.create();

		rangeTree.put(1, 20, "a");
		rangeTree.put(5, 15, "b");
		rangeTree.put(5, 10, "c");
		rangeTree.put(10, 15, "d");
		assertEquals(Stream.of("a", "b", "c", "d").collect(toSet()), rangeTree.getRange(0, 20));

		rangeTree.remove(5, 10, "c");
		rangeTree.remove(10, 15, "d");
		rangeTree.remove(1, 20, "a");
		rangeTree.remove(5, 15, "b");
		assertEquals(Stream.of().collect(toSet()), rangeTree.getRange(0, 20));
		assertTrue(rangeTree.getSegments().isEmpty());
	}
}
