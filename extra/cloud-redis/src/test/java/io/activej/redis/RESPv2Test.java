package io.activej.redis;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.redis.RedisResponse.ERROR;
import static io.activej.redis.TestUtils.assertDeepEquals;
import static org.junit.Assert.*;

public final class RESPv2Test {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void nil() {
		byte[] bytes = doDecode(encodeNil(true), RedisResponse.BYTES);
		assertNull(bytes);

		Object[] array = doDecode(encodeNil(false), RedisResponse.ARRAY);
		assertNull(array);
	}

	@Test
	public void string() {
		test("", RedisResponse.STRING);
		test("hello", RedisResponse.STRING);
	}

	@Test
	public void bytes() {
		test("".getBytes(), RedisResponse.BYTES);
		test("hello".getBytes(), RedisResponse.BYTES);
		test("hello \r\n world".getBytes(), RedisResponse.BYTES);
	}

	@Test
	public void integer() {
		test(0L, RedisResponse.LONG);
		test(Long.MIN_VALUE, RedisResponse.LONG);
		test(Long.MAX_VALUE, RedisResponse.LONG);
	}

	@Test
	public void error() {
		test(new ServerError(""), ERROR);
		test(new ServerError("ERROR"), ERROR);
		test(new ServerError("ERROR : something went wrong"), ERROR);
	}

	@Test
	public void array() {
		test(new Object[0], RedisResponse.ARRAY);
		test(new Object[]{null}, RedisResponse.ARRAY);
		test(new Object[]{""}, RedisResponse.ARRAY);
		test(new Object[]{"test"}, RedisResponse.ARRAY);
		test(new Object[]{0L}, RedisResponse.ARRAY);
		test(new Object[]{"first", "second"}, RedisResponse.ARRAY);
		test(new Object[]{"first", 2L, "third", 4L}, RedisResponse.ARRAY);
		test(new Object[]{"first", 2L, null, "third", 4L}, RedisResponse.ARRAY);
	}

	@Test
	public void arrayOfArrays() {
		test(new Object[]{new Object[0]}, RedisResponse.ARRAY);
		test(new Object[]{new Object[0], new Object[0]}, RedisResponse.ARRAY);
		test(new Object[]{null, new Object[0], null}, RedisResponse.ARRAY);
		test(new Object[]{
						new Object[]{"test1"},
						new Object[]{"test2"},
						new Object[]{"test3"}
				},
				RedisResponse.ARRAY);
		test(new Object[]{
						"Hello",
						new Object[]{
								null,
								new Object[]{"inner"},
								"test",
								100L,
								new byte[]{1, 2, 3}
						},
						null,
						new Object[]{
								1L,
								"",
								"test",
								new Object[0]
						}
				},
				RedisResponse.ARRAY);
		test(new Object[]{new Object[]{new Object[]{new Object[]{new Object[]{new Object[]{"test"}}}}}}, RedisResponse.ARRAY);
	}

	private static byte[] encode(Object object) {
		if (object instanceof Integer) {
			return doEncode(((Integer) object).longValue());
		} else if (object instanceof Long) {
			return doEncode((Long) object);
		} else if (object instanceof String) {
			return doEncode((String) object);
		} else if (object instanceof byte[]) {
			return doEncode((byte[]) object);
		} else if (object instanceof ServerError) {
			return doEncode((ServerError) object);
		} else if (object instanceof Object[]) {
			return doEncode((Object[]) object);
		} else {
			throw new AssertionError();
		}
	}

	private static <T> void test(T object, RedisResponse<T> response) {
		doTest(object, response, false);
		doTest(object, response, true);
	}

	private static <T> void doTest(Object object, RedisResponse<T> response, boolean incr) {
		byte[] encoded = encode(object);

		int len = incr ? 1 : encoded.length;

		T parsed = null;
		while (len <= encoded.length) {
			try {
				parsed = response.parse(new RESPv2(encoded, 0, len));
				break;
			} catch (MalformedDataException e) {
				throw new AssertionError(e);
			} catch (NeedMoreDataException e) {
				len++;
			}
		}
		assert parsed != null;
		if (object instanceof byte[]) {
			assertArrayEquals((byte[]) object, (byte[]) parsed);
		} else if (object instanceof Object[]) {
			assertDeepEquals((Object[]) object, (Object[]) parsed);
		} else if (object instanceof ServerError) {
			assertEquals(((ServerError) object).getMessage(), ((ServerError) parsed).getMessage());
		} else {
			assertEquals(object, parsed);
		}
		assertEquals(encoded.length, len);
	}

	private static <T> T doDecode(byte[] responseBytes, RedisResponse<T> response) {
		RESPv2 resp = new RESPv2(responseBytes, 0, responseBytes.length);
		try {
			return response.parse(resp);
		} catch (MalformedDataException e) {
			throw new AssertionError(e);
		}
	}

	private static byte[] encodeNil(boolean bulk) {
		return new byte[]{(byte) (bulk ? '$' : '*'), '-', '1', 13, 10};
	}

	private static byte[] doEncode(String string) {
		byte[] result = new byte[string.length() + 3];
		result[0] = '+';
		encodeAscii(result, 1, string);
		result[result.length - 2] = CR;
		result[result.length - 1] = LF;
		return result;
	}

	private static byte[] doEncode(byte[] bytes) {
		byte[] sizeBytes = String.valueOf(bytes.length).getBytes();
		byte[] result = new byte[bytes.length + sizeBytes.length + 5];
		result[0] = '$';
		System.arraycopy(sizeBytes, 0, result, 1, sizeBytes.length);
		result[sizeBytes.length + 1] = CR;
		result[sizeBytes.length + 2] = LF;
		System.arraycopy(bytes, 0, result, 3 + sizeBytes.length, bytes.length);
		result[result.length - 2] = CR;
		result[result.length - 1] = LF;
		return result;
	}

	private static byte[] doEncode(Long integer) {
		byte[] bytes = String.valueOf(integer).getBytes();
		byte[] result = new byte[bytes.length + 3];
		result[0] = ':';
		System.arraycopy(bytes, 0, result, 1, bytes.length);
		result[result.length - 2] = CR;
		result[result.length - 1] = LF;

		return result;
	}

	private static byte[] doEncode(Object[] array) {
		ByteBufs bufs = new ByteBufs();
		doEncode(array, bufs);
		return bufs.takeRemaining().asArray();
	}

	private static void doEncode(Object[] array, ByteBufs bufs) {
		bufs.add(ByteBuf.wrapForReading(new byte[]{'*'}));
		bufs.add(ByteBuf.wrapForReading(String.valueOf(array.length).getBytes()));
		bufs.add(ByteBuf.wrapForReading(new byte[]{13, 10}));
		for (Object o : array) {
			if (o == null) {
				bufs.add(ByteBuf.wrapForReading(encodeNil(ThreadLocalRandom.current().nextBoolean())));
			} else if (o instanceof Long) {
				bufs.add(ByteBuf.wrapForReading(doEncode((Long) o)));
			} else if (o instanceof String) {
				bufs.add(ByteBuf.wrapForReading(doEncode((String) o)));
			} else if (o instanceof byte[]) {
				bufs.add(ByteBuf.wrapForReading(doEncode((byte[]) o)));
			} else if (o instanceof ServerError) {
				bufs.add(ByteBuf.wrapForReading(doEncode((ServerError) o)));
			} else {
				assert o instanceof Object[];
				doEncode((Object[]) o, bufs);
			}
		}
	}

	private static byte[] doEncode(ServerError error) {
		String string = error.getMessage();
		byte[] result = new byte[string.length() + 3];
		result[0] = '-';
		encodeAscii(result, 1, string);
		result[result.length - 2] = CR;
		result[result.length - 1] = LF;
		return result;
	}
}
