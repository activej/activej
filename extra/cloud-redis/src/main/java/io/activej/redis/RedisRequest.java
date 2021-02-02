package io.activej.redis;

import io.activej.common.Checks;
import io.activej.common.collection.CollectionUtils;

import java.util.Collections;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.redis.RESPv2.ARRAY_MARKER;
import static io.activej.redis.RESPv2.BYTES_MARKER;

public abstract class RedisRequest {
	public static final boolean CHECK = Checks.isEnabled(RedisRequest.class);

	public abstract int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException;

	public static RedisRequest of(Object cmd) {
		return new RedisRequest() {
			@Override
			public int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException {
				return writeRequest(Collections.singletonList(cmd), array, offset);
			}
		};
	}

	public static RedisRequest of(Object cmd, Object arg1) {
		return new RedisRequest() {
			@Override
			public int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException {
				return writeRequest(CollectionUtils.list(cmd, arg1), array, offset);
			}
		};
	}

	public static RedisRequest of(Object... args) {
		return new RedisRequest() {
			@Override
			public int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException {
				array[offset++] = ARRAY_MARKER;

				offset += encodePositiveInt(array, offset, args.length);
				array[offset++] = CR;
				array[offset++] = LF;

				for (Object arg : args) {
					offset = writeArgument(array, offset, arg);
				}

				return offset;
			}
		};
	}

	public static RedisRequest of(List<Object> args) {
		return new RedisRequest() {
			@Override
			public int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException {
				return writeRequest(args, array, offset);
			}
		};
	}

	private static int writeRequest(List<Object> args, byte[] array, int offset) {
		array[offset++] = ARRAY_MARKER;

		offset += encodePositiveInt(array, offset, args.size());
		array[offset++] = CR;
		array[offset++] = LF;

		for (Object arg : args) {
			offset = writeArgument(array, offset, arg);
		}

		return offset;
	}

	private static int writeArgument(byte[] array, int offset, Object arg) {
		array[offset++] = BYTES_MARKER;
		if (arg instanceof String) {
			String str = (String) arg;
			offset = writeString(array, offset, str);
		} else if (arg instanceof byte[]) {
			byte[] bytes = (byte[]) arg;
			offset = writeBytes(array, offset, bytes);
		} else {
			throw new IllegalArgumentException();
		}
		array[offset++] = CR;
		array[offset++] = LF;
		return offset;
	}

	private static int writeString(byte[] array, int offset, String str) {
		offset += encodePositiveInt(array, offset, str.length());
		array[offset++] = CR;
		array[offset++] = LF;
		for (int j = 0; j < str.length(); j++) {
			char c = str.charAt(j);
			if (CHECK && (c < 0x20 || c >= 0x80)) throw new IllegalArgumentException();
			array[offset++] = (byte) c;
		}
		return offset;
	}

	private static int writeBytes(byte[] array, int offset, byte[] bytes) {
		offset += encodePositiveInt(array, offset, bytes.length);
		array[offset++] = CR;
		array[offset++] = LF;
		System.arraycopy(bytes, 0, array, offset, bytes.length);
		offset += bytes.length;
		return offset;
	}

}
