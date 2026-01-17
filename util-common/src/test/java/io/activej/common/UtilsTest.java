package io.activej.common;

import io.activej.common.collection.IteratorUtils;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UtilsTest {
	@Test
	public void testIteratorsConcat() {
		List<Integer> list1 = Arrays.asList(1, 2, 3);
		List<Integer> list2 = Arrays.asList(3, 4, 5);

		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		List<Integer> result = new ArrayList<>();
		concat.forEachRemaining(result::add);

		assertEquals(Arrays.asList(1, 2, 3, 3, 4, 5), result);
	}

	@Test
	public void testIteratorsConcatWithEmpty() {
		List<Integer> list1 = Collections.emptyList();
		List<Integer> list2 = Arrays.asList(3, 4, 5);

		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		List<Integer> result = new ArrayList<>();
		concat.forEachRemaining(result::add);

		assertEquals(Arrays.asList(3, 4, 5), result);
	}

	@Test
	public void testIteratorsConcatBothEmpty() {
		List<Integer> list1 = Collections.emptyList();
		List<Integer> list2 = Collections.emptyList();

		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		List<Integer> result = new ArrayList<>();
		concat.forEachRemaining(result::add);

		assertTrue(result.isEmpty());
	}

	@Test
	public void testIteratorsConcatSingleElement() {
		List<Integer> list1 = Collections.singletonList(1);
		List<Integer> list2 = Collections.singletonList(2);

		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		List<Integer> result = new ArrayList<>();
		concat.forEachRemaining(result::add);

		assertEquals(Arrays.asList(1, 2), result);
	}

	@Test
	public void testIteratorsConcatWithNullElements() {
		List<Integer> list1 = Arrays.asList(1, null, 3);
		List<Integer> list2 = Arrays.asList(null, 4, 5);

		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		List<Integer> result = new ArrayList<>();
		concat.forEachRemaining(result::add);

		assertEquals(Arrays.asList(1, null, 3, null, 4, 5), result);
	}

	@Test
	public void testIteratorsConcatWithHasNext() {
		List<Integer> list1 = Arrays.asList(1, 2, 3);
		List<Integer> list2 = Arrays.asList(4, 5, 6);

		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		assertTrue(concat.hasNext());
		assertEquals((Integer) 1, concat.next());
		assertEquals((Integer) 2, concat.next());
		assertEquals((Integer) 3, concat.next());
		assertEquals((Integer) 4, concat.next());
		assertEquals((Integer) 5, concat.next());
		assertEquals((Integer) 6, concat.next());
		assertFalse(concat.hasNext());
	}
}
