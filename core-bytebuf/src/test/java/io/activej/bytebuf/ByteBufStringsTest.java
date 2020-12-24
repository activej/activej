package io.activej.bytebuf;

import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.activej.bytebuf.ByteBufStrings.decodeUtf8;
import static io.activej.bytebuf.ByteBufStrings.encodeLong;
import static io.activej.bytebuf.ByteBufTest.initByteBufPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ByteBufStringsTest {
	static {
		initByteBufPool();
	}

	private static final Random RANDOM = new Random();

	@Test
	public void testEncodeLong() throws MalformedDataException {
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[20]);

		// Test edge cases
		encodeLongTest(buf, Long.MAX_VALUE);
		encodeLongTest(buf, Long.MIN_VALUE);

		// Test random values
		long value = RANDOM.nextLong();
		byte[] bytes = String.valueOf(value).getBytes();
		Arrays.fill(bytes, (byte) 0);
		buf = ByteBuf.wrapForWriting(bytes);
		encodeLongTest(buf, value);
	}

	@Test
	public void testDecodeInt() throws MalformedDataException {
		// Test edge cases
		decodeIntTest(Integer.MAX_VALUE);
		decodeIntTest(Integer.MIN_VALUE);

		// Test random values
		decodeIntTest(RANDOM.nextInt());
	}

	@Test
	public void testDecodeLong() throws MalformedDataException {
		// Test edge cases
		decodeLongTest(Long.MAX_VALUE);
		decodeLongTest(Long.MIN_VALUE);

		// Test random values
		decodeLongTest(RANDOM.nextLong());

		// Test with offset
		String string = "-1234567891234";
		byte[] bytesRepr = string.getBytes();
		long decoded = ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
		assertEquals(-1234567891234L, decoded);

		// Test bigger than long
		try {
			string = "92233720368547758081242123";
			bytesRepr = string.getBytes();
			ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
			fail();
		} catch (MalformedDataException e) {
			assertEquals("Bigger than max long value: 92233720368547758081242123", e.getMessage());
		}
	}

	@Test
	public void testWrapLong() throws MalformedDataException {
		// Test edge cases
		ByteBuf byteBuf = ByteBufStrings.wrapLong(Long.MAX_VALUE);
		assertEquals(String.valueOf(Long.MAX_VALUE), ByteBufStrings.decodeUtf8(byteBuf));

		byteBuf = ByteBufStrings.wrapLong(Long.MIN_VALUE);
		assertEquals(String.valueOf(Long.MIN_VALUE), ByteBufStrings.decodeUtf8(byteBuf));

		long value = RANDOM.nextLong();
		byteBuf = ByteBufStrings.wrapLong(value);
		assertEquals(String.valueOf(value), ByteBufStrings.decodeUtf8(byteBuf));
	}

	// region helpers
	private void encodeLongTest(ByteBuf buf, long value) throws MalformedDataException {
		buf.rewind();
		buf.moveTail(encodeLong(buf.array, buf.head(), value));
		String stringRepr = decodeUtf8(buf);
		assertEquals(String.valueOf(value), stringRepr);
	}

	private void decodeIntTest(int value) throws MalformedDataException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		int decoded = ByteBufStrings.decodeInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void decodeLongTest(long value) throws MalformedDataException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		long decoded = ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}
	// endregion
}
