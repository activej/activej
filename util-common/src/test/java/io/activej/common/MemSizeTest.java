package io.activej.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MemSizeTest {
	@Test
	public void testMemSize() {
		long bytes;

		bytes = 0;
		assertEquals(bytes + "", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("0 b").toLong());

		bytes = 512;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("512").toLong());

		bytes = 1024;
		assertEquals("1Kb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1kb").toLong());

		bytes = 2048;
		assertEquals("2Kb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 k 1024b").toLong());

		bytes = 1025;
		assertEquals("" + bytes, MemSize.of(bytes).format());

		bytes = 1024L * 1024L;
		assertEquals("1Mb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 mb").toLong());
		assertEquals(bytes, MemSize.valueOf("1024kb").toLong());

		bytes = 1024L * 1024L + 15;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 m 15").toLong());

		bytes = 1024L * 1024L * 10;
		assertEquals("10Mb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("10mb").toLong());

		bytes = 1024L * 1024L * 10 - 1;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9 m 1023kb 1023b").toLong());

		bytes = 1024L * 1024L * 1024L;
		assertEquals("1Gb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1gb").toLong());

		bytes = 1024L * 1024L * 1024L + 15;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1g  15 b").toLong());

		bytes = 1024L * 1024L * 1024L * 10;
		assertEquals("10Gb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("10gb").toLong());

		bytes = 1024L * 1024L * 1024L * 10 - 1;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9gb 1023 b 1023mb 1023kb").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L;
		assertEquals("1Tb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 TB").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L + 15;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1Tb 15B").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L * 10;
		assertEquals("10Tb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9tB 1024 G").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L * 10 - 1;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9 tb 1023 G 1023 mB 1023KB 1023B").toLong());

		assertEquals(228, MemSize.valueOf("228").toLong());
		assertEquals(1024 + 228, MemSize.valueOf("228 1 kb").toLong());
		assertEquals(1536, MemSize.valueOf("1.5kb").toLong());
		assertEquals(1024 * 1024 + 512 * 1024, MemSize.valueOf("1.5 mB").toLong());
		assertEquals(1024L * 1024L * 1024L + 512L * 1024L * 1024L, MemSize.valueOf("1.5 Gb").toLong());
		assertEquals(1024L * 1024L * 1024L * 1024L + 512L * 1024L * 1024L * 1024L, MemSize.valueOf("1.5 TB").toLong());
		assertEquals("2000000", MemSize.of(2000000L).toString());

		//      2 tb                                3 gb                        228 mb                1 b
		bytes = 1024L * 1024L * 1024L * 1024L * 2 + 1024L * 1024L * 1024L * 3 + 1024L * 1024L * 228 + 1;
		assertEquals(MemSize.valueOf("2 Tb 3gb 1b 228mb").format(), MemSize.of(bytes).format());

		MemSize memSize = MemSize.kilobytes(1423998);
		assertEquals(1458173952L, memSize.toLong());
		assertEquals("1423998Kb", StringFormatUtils.formatMemSize(memSize));
	}

	@Test
	public void testHumanReadableFormat() {
		assertEquals("0", StringFormatUtils.formatMemSizeHumanReadable(MemSize.bytes(0)));
		assertEquals("1", StringFormatUtils.formatMemSizeHumanReadable(MemSize.bytes(1)));

		assertEquals("100", StringFormatUtils.formatMemSizeHumanReadable(MemSize.bytes(100)));

		assertEquals("1023", StringFormatUtils.formatMemSizeHumanReadable(MemSize.kilobytes(1).map(x -> x - 1)));
		assertEquals("1024", StringFormatUtils.formatMemSizeHumanReadable(MemSize.kilobytes(1)));
		assertEquals("1025", StringFormatUtils.formatMemSizeHumanReadable(MemSize.kilobytes(1).map(x -> x + 1)));

		assertEquals("10239", StringFormatUtils.formatMemSizeHumanReadable(MemSize.kilobytes(10).map(x -> x - 1)));
		assertEquals("10Kb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.kilobytes(10)));
		assertEquals("10Kb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.kilobytes(10).map(x -> x + 1)));

		assertEquals("10239Kb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.megabytes(10).map(x -> x - 1)));
		assertEquals("10Mb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.megabytes(10)));
		assertEquals("10Mb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.megabytes(10).map(x -> x + 1)));

		assertEquals("5119Gb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.terabytes(5).map(x -> x - 1)));
		assertEquals("5120Gb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.terabytes(5)));
		assertEquals("5120Gb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.terabytes(5).map(x -> x + 1)));

		assertEquals("10239Tb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.terabytes(10 * 1024).map(x -> x - 1)));
		assertEquals("10240Tb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.terabytes(10 * 1024)));
		assertEquals("10240Tb", StringFormatUtils.formatMemSizeHumanReadable(MemSize.terabytes(10 * 1024).map(x -> x + 1)));
	}

	@Test
	public void testParsingExceptions() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MemSize.valueOf("2.2b"));
		assertEquals("MemSize unit bytes cannot be fractional", e.getMessage());
	}

	@Test
	public void testLongOverflow() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MemSize.kilobytes(Long.MAX_VALUE));
		assertEquals("Resulting number of bytes exceeds Long.MAX_VALUE", e.getMessage());
	}

	@Test
	public void testMemSizeOfNegative() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MemSize.kilobytes(-1));
		assertEquals("Cannot create MemSize of negative value", e.getMessage());
	}
}
