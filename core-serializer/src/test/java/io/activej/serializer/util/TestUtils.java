package io.activej.serializer.util;

import org.junit.Test;

import static io.activej.serializer.util.Utils.extractRanges;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class TestUtils {
	@Test
	public void testExtractRanges() {
		assertEquals("1,2", extractRanges(asList(1, 2)));
		assertEquals("1-3", extractRanges(asList(1, 2, 3)));
		assertEquals("1-3,5", extractRanges(asList(1, 2, 3, 5)));
		assertEquals("1-5", extractRanges(asList(1, 2, 3, 4, 5)));
		assertEquals("1-3,5,6", extractRanges(asList(1, 2, 3, 5, 6)));
		assertEquals("1-5,7", extractRanges(asList(1, 2, 3, 4, 5, 7)));
		assertEquals("1-3,5-7", extractRanges(asList(1, 2, 3, 5, 6, 7)));
		assertEquals("1,2,4,6,8-10", extractRanges(asList(1, 2, 4, 6, 8, 9, 10)));
		assertEquals("1-3,5,6,8-10", extractRanges(asList(1, 2, 3, 5, 6, 8, 9, 10)));
		assertEquals("1-3,5,6,8-10,13,14,16", extractRanges(asList(1, 2, 3, 5, 6, 8, 9, 10, 13, 14, 16)));
	}
}
