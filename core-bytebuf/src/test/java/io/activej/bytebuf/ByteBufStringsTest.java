package io.activej.bytebuf;

import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static io.activej.bytebuf.ByteBufStrings.decodeUtf8;
import static io.activej.bytebuf.ByteBufStrings.encodeLong;
import static io.activej.bytebuf.ByteBufTest.initByteBufPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ByteBufStringsTest {
	static {
		initByteBufPool();
	}

	private static final Random RANDOM = new Random();

	@Test
	public void testEncodeLong() throws MalformedDataException {
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[20]);

		// Test edge cases
		assertEncodeLong(buf, Long.MAX_VALUE);
		assertEncodeLong(buf, Long.MIN_VALUE);

		// Test random values
		for (int i = 0; i < 10; i++) { // Run multiple random tests for better coverage
			long value = RANDOM.nextLong();
			buf = ByteBuf.wrapForWriting(new byte[20]); // Reset buffer for each test
			assertEncodeLong(buf, value);
		}
	}

	@Test
	public void testDecodeInt() throws MalformedDataException {
		// Test edge cases
		assertDecodeInt(Integer.MAX_VALUE);
		assertDecodeInt(Integer.MIN_VALUE);

		// Test random values
		for (int i = 0; i < 10; i++) { // Run multiple random tests for better coverage
			assertDecodeInt(RANDOM.nextInt());
		}
	}

	@Test
	public void testDecodeLong() throws MalformedDataException {
		// Test edge cases
		assertDecodeLong(Long.MAX_VALUE);
		assertDecodeLong(Long.MIN_VALUE);

		// Test random values
		for (int i = 0; i < 10; i++) { // Run multiple random tests for better coverage
			assertDecodeLong(RANDOM.nextLong());
		}

		// Test with offset
		String string = "-1234567891234";
		byte[] bytesRepr = string.getBytes(StandardCharsets.UTF_8);
		long decoded = ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
		assertEquals(-1234567891234L, decoded);

		// Test with value bigger than Long.MAX_VALUE
		String largeNumber = "92233720368547758081242123";
		byte[] largeBytes = largeNumber.getBytes(StandardCharsets.UTF_8);
		MalformedDataException exception = assertThrows(MalformedDataException.class,
			() -> ByteBufStrings.decodeLong(largeBytes, 0, largeNumber.length()));
		assertEquals("Bigger than max long value: 92233720368547758081242123", exception.getMessage());
	}

	@Test
	public void testWrapLong() throws MalformedDataException {
		// Test edge cases
		assertWrapLong(Long.MAX_VALUE);
		assertWrapLong(Long.MIN_VALUE);

		// Test random values
		for (int i = 0; i < 10; i++) { // Run multiple random tests for better coverage
			assertWrapLong(RANDOM.nextLong());
		}
	}

	// region Helper Methods
	private void assertEncodeLong(ByteBuf buf, long value) throws MalformedDataException {
		buf.rewind();
		buf.moveTail(encodeLong(buf.array(), buf.head(), value));
		String stringRepr = decodeUtf8(buf);
		assertEquals("Encoded value did not match the expected string representation", String.valueOf(value), stringRepr);
	}

	private void assertDecodeInt(int value) throws MalformedDataException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes(StandardCharsets.UTF_8);
		int decoded = ByteBufStrings.decodeInt(bytesRepr, 0, string.length());
		assertEquals("Decoded value did not match the original", value, decoded);
	}

	private void assertDecodeLong(long value) throws MalformedDataException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes(StandardCharsets.UTF_8);
		long decoded = ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
		assertEquals("Decoded value did not match the original", value, decoded);
	}

	private void assertWrapLong(long value) throws MalformedDataException {
		ByteBuf byteBuf = ByteBufStrings.wrapLong(value);
		String decodedString = ByteBufStrings.decodeUtf8(byteBuf);
		assertEquals("Wrapped value did not match the expected string representation", String.valueOf(value), decodedString);
	}
	// endregion
}
