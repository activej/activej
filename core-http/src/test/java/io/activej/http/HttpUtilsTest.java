package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.test.rules.LambdaStatement.ThrowingRunnable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.activej.bytebuf.ByteBufStrings.decodeUtf8;
import static io.activej.bytebuf.ByteBufStrings.encodePositiveInt;
import static io.activej.http.HttpUtils.trimAndDecodePositiveInt;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public class HttpUtilsTest {
	private static final Random RANDOM = new Random();

	@Test
	public void testEncodePositiveDecimal() throws MalformedDataException {
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[20]);

		// Test edge cases
		encodePositiveIntTest(buf, Integer.MAX_VALUE);

		// Test random values
		int value = Math.abs(RANDOM.nextInt());
		byte[] bytes = String.valueOf(value).getBytes();
		Arrays.fill(bytes, (byte) 0);
		buf = ByteBuf.wrapForWriting(bytes);
		encodePositiveIntTest(buf, value);
	}

	@Test
	public void testDecodePositiveInt() throws MalformedHttpException {
		// Test edge cases
		decodeUnsignedIntTest(Integer.MAX_VALUE);
		decodeUnsignedIntTest(0);

		// Test random values
		decodeUnsignedIntTest(Math.abs(RANDOM.nextInt()));
	}

	@Test
	public void testDecodePositiveInt2() throws MalformedHttpException {
		// Test edge cases
		decodeUnsignedLongTest(Integer.MAX_VALUE);
		decodeUnsignedLongTest(0);

		// Test random values
		decodeUnsignedLongTest(Math.abs(RANDOM.nextInt()));

		// Test with offset
		String string = "  \t\t  123456789 \t\t\t\t   ";
		byte[] bytesRepr = string.getBytes();
		int decoded = trimAndDecodePositiveInt(bytesRepr, 0, string.length());
		assertEquals(123456789, decoded);

		// Test bigger than long
		try {
			string = "  \t\t  92233720368547758081242123 \t\t\t\t   ";
			bytesRepr = string.getBytes();
			trimAndDecodePositiveInt(bytesRepr, 0, string.length());
			fail();
		} catch (MalformedHttpException e) {
			assertEquals("Bigger than max int value: 92233720368547758081242123", e.getMessage());
		}
	}

	@Test
	public void testNegativeValueWithOffset() {
		String text = "Content-Length: -1";
		byte[] bytes = text.getBytes();
		assertNegativeSizeException(() -> HttpUtils.trimAndDecodePositiveInt(bytes, 15, 3));
		assertNegativeSizeException(() -> HttpUtils.trimAndDecodePositiveInt(bytes, 16, 2));
	}

	// region helpers
	private void encodePositiveIntTest(ByteBuf buf, int value) throws MalformedDataException {
		buf.rewind();
		buf.moveTail(encodePositiveInt(buf.array(), buf.head(), value));
		String stringRepr = decodeUtf8(buf);
		assertEquals(String.valueOf(value), stringRepr);
	}

	private void decodeUnsignedIntTest(int value) throws MalformedHttpException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		int decoded = trimAndDecodePositiveInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void decodeUnsignedLongTest(int value) throws MalformedHttpException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		long decoded = HttpUtils.decodePositiveInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void assertNegativeSizeException(ThrowingRunnable runnable) {
		try {
			runnable.run();
			fail();
		} catch (Throwable e) {
			assertThat(e, instanceOf(MalformedHttpException.class));
			assertThat(e.getMessage(), containsString("Not a decimal value"));
		}

	}
	// endregion
}
