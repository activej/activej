package io.activej.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MemSizeTest {

	private void testFormat(long bytes, String expectedFormat) {
		assertEquals(expectedFormat, MemSize.of(bytes).format());
	}

	private void testParsing(String input, long expectedBytes) {
		assertEquals(expectedBytes, MemSize.valueOf(input).toLong());
	}

	@Test
	public void testMemSize() {
		// Formatting tests
		testFormat(0, "0");
		testFormat(512, "512");
		testFormat(1024, "1Kb");
		testFormat(2048, "2Kb");
		testFormat(1025, "1025");
		testFormat(1536, "1536");

		testFormat(1024L * 1024L, "1Mb");
		testFormat(1024L * 1024L + 15, "1048591");

		testFormat(1024L * 1024L * 10, "10Mb");
		testFormat(1024L * 1024L * 10 - 1, "10485759");

		testFormat(1024L * 1024L * 1024L, "1Gb");
		testFormat(1024L * 1024L * 1024L + 15, "1073741839");

		testFormat(1024L * 1024L * 1024L * 10, "10Gb");
		testFormat(1024L * 1024L * 1024L * 10 - 1, "10737418239");

		testFormat(1024L * 1024L * 1024L * 1024L, "1Tb");
		testFormat(1024L * 1024L * 1024L * 1024L + 15, "1099511627791");

		testFormat(1024L * 1024L * 1024L * 1024L * 10, "10Tb");
		testFormat(1024L * 1024L * 1024L * 1024L * 10 - 1, "10995116277759");

		// Parsing tests
		testParsing("0 b", 0);
		testParsing("512", 512);
		testParsing("1kb", 1024);
		testParsing("1 k 1024b", 2048);
		testParsing("1 mb", 1024L * 1024L);
		testParsing("1024kb", 1024L * 1024L);
		testParsing("1 m 15", 1024L * 1024L + 15);
		testParsing("10mb", 1024L * 1024L * 10);
		testParsing("9 m 1023kb 1023b", 1024L * 1024L * 10 - 1);
		testParsing("1gb", 1024L * 1024L * 1024L);
		testParsing("1g  15 b", 1024L * 1024L * 1024L + 15);
		testParsing("10gb", 1024L * 1024L * 1024L * 10);
		testParsing("9gb 1023 b 1023mb 1023kb", 1024L * 1024L * 1024L * 10 - 1);
		testParsing("1 TB", 1024L * 1024L * 1024L * 1024L);
		testParsing("1Tb 15B", 1024L * 1024L * 1024L * 1024L + 15);
		testParsing("9tB 1024 G", 1024L * 1024L * 1024L * 1024L * 10);
		testParsing("9 tb 1023 G 1023 mB 1023KB 1023B", 1024L * 1024L * 1024L * 1024L * 10 - 1);
		testParsing("228", 228);
		testParsing("228 1 kb", 1024 + 228);
		testParsing("1.5kb", 1536);
		testParsing("1.5 mB", 1024 * 1024 + 512 * 1024);
		testParsing("1.5 Gb", 1024L * 1024L * 1024L + 512L * 1024L * 1024L);
		testParsing("1.5 TB", 1024L * 1024L * 1024L * 1024L + 512L * 1024L * 1024L * 1024L);

		// Testing toString()
		assertEquals("2000000", MemSize.of(2000000L).toString());

		// Combined parsing and formatting test
		long bytes = 1024L * 1024L * 1024L * 1024L * 2 + 1024L * 1024L * 1024L * 3 + 1024L * 1024L * 228 + 1;
		MemSize memSize = MemSize.valueOf("2 Tb 3gb 1b 228mb");
		assertEquals(bytes, memSize.toLong());
		assertEquals(MemSize.of(bytes).format(), memSize.format());

		// Additional test case
		memSize = MemSize.kilobytes(1423998);
		assertEquals(1458173952L, memSize.toLong());
		assertEquals("1423998Kb", StringFormatUtils.formatMemSize(memSize));
	}

	@Test
	public void testHumanReadableFormat() {
		Object[][] testCases = new Object[][]{
			{MemSize.bytes(0), "0"},
			{MemSize.bytes(1), "1"},
			{MemSize.bytes(100), "100"},
			{MemSize.kilobytes(1).map(x -> x - 1), "1023"},
			{MemSize.kilobytes(1), "1024"},
			{MemSize.kilobytes(1).map(x -> x + 1), "1025"},
			{MemSize.kilobytes(10).map(x -> x - 1), "10239"},
			{MemSize.kilobytes(10), "10Kb"},
			{MemSize.kilobytes(10).map(x -> x + 1), "10Kb"},
			{MemSize.megabytes(10).map(x -> x - 1), "10239Kb"},
			{MemSize.megabytes(10), "10Mb"},
			{MemSize.megabytes(10).map(x -> x + 1), "10Mb"},
			{MemSize.terabytes(5).map(x -> x - 1), "5119Gb"},
			{MemSize.terabytes(5), "5120Gb"},
			{MemSize.terabytes(5).map(x -> x + 1), "5120Gb"},
			{MemSize.terabytes(10 * 1024).map(x -> x - 1), "10239Tb"},
			{MemSize.terabytes(10 * 1024), "10240Tb"},
			{MemSize.terabytes(10 * 1024).map(x -> x + 1), "10240Tb"},
		};

		for (Object[] testCase : testCases) {
			MemSize memSize = (MemSize) testCase[0];
			String expected = (String) testCase[1];
			assertEquals(expected, StringFormatUtils.formatMemSizeHumanReadable(memSize));
		}
	}

	@Test
	public void testParsingExceptions() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("2.2b"));
		assertEquals("MemSize unit bytes cannot be fractional", e.getMessage());

		// Additional invalid inputs
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("abc"));
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("1.5.5kb"));
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("1kb1mb"));
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("-1kb"));
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("1kb -1b"));
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("1kb xyz"));
	}

	@Test
	public void testLongOverflow() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MemSize.kilobytes(Long.MAX_VALUE));
		assertEquals("Resulting number of bytes exceeds Long.MAX_VALUE", e.getMessage());

		// Additional test for overflow in parsing
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("9223372036854775808b")); // Long.MAX_VALUE + 1
		assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("1024Pb")); // Exceeds Long.MAX_VALUE
	}

	@Test
	public void testMemSizeOfNegative() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MemSize.kilobytes(-1));
		assertEquals("Cannot create MemSize of negative value", e.getMessage());

		// Additional negative values
		assertThrows(IllegalArgumentException.class, () -> MemSize.bytes(-100));
		assertThrows(IllegalArgumentException.class, () -> MemSize.megabytes(-5));
		assertThrows(IllegalArgumentException.class, () -> MemSize.gigabytes(-10));
		assertThrows(IllegalArgumentException.class, () -> MemSize.terabytes(-2));
	}
}
