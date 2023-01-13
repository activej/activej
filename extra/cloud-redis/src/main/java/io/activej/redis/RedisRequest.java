/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.redis;

import io.activej.common.Checks;

import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.redis.RESPv2.ARRAY_MARKER;
import static io.activej.redis.RESPv2.BYTES_MARKER;

/**
 * Redis request that will be serialized and written to the server.
 * There are some predefined static methods that can be used for building a request.
 * A user can extend this class to provide a more specialized request and a more
 * efficient way of writing request data to the buffer array.
 */
public abstract class RedisRequest {
	public static final boolean CHECKS = Checks.isEnabled(RedisRequest.class);

	/**
	 * Serializes this request and writes to the provided array.
	 *
	 * @param array  an array containing request data which will be sent to the Redis server
	 * @param offset offset, starting from which a serialized request should be written to the array
	 * @return a new offset after writing serialized request to the array
	 * @throws ArrayIndexOutOfBoundsException thrown when there is not enough space in the provided array
	 *                                        to hold a serialized request
	 */
	public abstract int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException;

	/**
	 * Creates a RedisRequest that contains a Redis command with no parameters.
	 * <b>Only {@code String} or {@code byte[]} argument is supported</b>
	 *
	 * @param cmd Redis command to be serialized
	 * @return RedisRequest of a Redis command with no parameters
	 */
	public static RedisRequest of(Object cmd) {
		return new RedisRequest() {
			@Override
			public int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException {
				return writeRequest(List.of(cmd), array, offset);
			}
		};
	}

	/**
	 * Creates a RedisRequest that contains a Redis command with a single parameter.
	 * <b>Only {@code String} or {@code byte[]} arguments are supported</b>
	 *
	 * @param cmd  Redis command to be serialized
	 * @param arg1 a parameter to a Redis command
	 * @return RedisRequest of a Redis command with a single parameter
	 */
	public static RedisRequest of(Object cmd, Object arg1) {
		return new RedisRequest() {
			@Override
			public int write(byte[] array, int offset) throws ArrayIndexOutOfBoundsException {
				return writeRequest(List.of(cmd, arg1), array, offset);
			}
		};
	}

	/**
	 * Creates a RedisRequest that contains a Redis command with an arbitrary number of parameters.
	 * <b>Array may hold only {@code String} or {@code byte[]} elements, may be mixed</b>
	 *
	 * @param args an array that contains Redis command and its parameters. <b>Cannot be empty.</b>
	 * @return RedisRequest of a Redis command with an arbitrary number of parameters
	 */
	public static RedisRequest of(Object... args) {
		if (CHECKS) checkArgument(args.length != 0, "No Redis command is present");

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

	/**
	 * Creates a RedisRequest that contains a Redis command with an arbitrary number of parameters.
	 * <b>List may hold only {@code String} or {@code byte[]} elements, may be mixed</b>
	 *
	 * @param args a list that contains Redis command and its parameters. <b>Cannot be empty</b>.
	 * @return RedisRequest of a Redis command with an arbitrary number of parameters
	 */
	public static RedisRequest of(List<Object> args) {
		if (CHECKS) checkArgument(!args.isEmpty(), "No Redis command is present");

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
		if (arg instanceof String str) {
			offset = writeString(array, offset, str);
		} else if (arg instanceof byte[] bytes) {
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
			if (CHECKS && (c < 0x20 || c >= 0x80)) throw new IllegalArgumentException();
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
