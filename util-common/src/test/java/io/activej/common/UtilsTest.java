package io.activej.common;

import io.activej.common.collection.IteratorUtils;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

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

		//noinspection RedundantOperationOnEmptyContainer
		Iterator<Integer> concat = IteratorUtils.concat(list1.iterator(), list2.iterator());
		List<Integer> result = new ArrayList<>();
		concat.forEachRemaining(result::add);

		assertEquals(Arrays.asList(3, 4, 5), result);
	}
}
