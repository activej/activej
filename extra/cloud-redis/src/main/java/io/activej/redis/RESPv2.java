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

import io.activej.common.ApplicationSettings;
import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static io.activej.redis.NeedMoreDataException.NEED_MORE_DATA;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * A Redis response buffer that holds a byte array of received responses from the server.
 * Contains several helpful methods for parsing Redis responses.
 */
public final class RESPv2 {
	/**
	 * This setting controls whether responses received from a server should be validated.
	 * By default, a server is considered to be trusted, so the setting is turned <b>OFF</b>
	 * for a minor performance boost
	 */
	public static final boolean ASSERT_PROTOCOL = ApplicationSettings.getBoolean(RESPv2.class, "assertProtocol", false);

	public static final byte STRING_MARKER = '+';
	public static final byte ERROR_MARKER = '-';
	public static final byte LONG_MARKER = ':';
	public static final byte BYTES_MARKER = '$';
	public static final byte ARRAY_MARKER = '*';

	public static final String NO_AUTH_PREFIX = "NOAUTH";
	public static final String ERR_AUTH_PREFIX = "ERR AUTH";
	public static final String NO_PERM_PREFIX = "NOPERM";

	private final byte[] array;
	private int head;
	private final int tail;

	RESPv2(byte[] array, int head, int tail) {
		this.array = array;
		this.head = head;
		this.tail = tail;
	}

	/**
	 * Returns an underlying byte array that contains raw responses from a server
	 *
	 * @return underlying array
	 */
	public byte[] array() {
		return array;
	}

	/**
	 * Returns a current {@code head} position of the buffer
	 *
	 * @return head position
	 */
	public int head() {
		return head;
	}

	/**
	 * Sets a current {@code head} position to the new position
	 *
	 * @param head new head position
	 */
	public void head(int head) {
		this.head = head;
	}

	/**
	 * Change a current {@code head} position by some delta
	 *
	 * @param delta amount by which head will be changed
	 */
	public void moveHead(int delta) {
		this.head += delta;
	}

	/**
	 * Returns a current {@code tail} position of the buffer
	 *
	 * @return tail position
	 */
	public int tail() {
		return tail;
	}

	/**
	 * Returns a remaining amount of unread bytes in the buffer
	 *
	 * @return amount of unread bytes
	 */
	public int readRemaining() {
		return head - tail;
	}

	/**
	 * Indicates whether there are any unread bytes in the buffer
	 *
	 * @return whether there are bytes to be read from the buffer
	 */
	public boolean canRead() {
		return head < tail;
	}

	/**
	 * Returns the next byte in the buffer without changing a position of head
	 *
	 * @return next byte in the buffer
	 */
	public byte peek() {
		return array[head];
	}

	/**
	 * Reads an arbitrary Object from the buffer.
	 * <p>
	 * May return either of:
	 * <ul>
	 *     <li><b>{@code String}</b> - Redis Simple String</li>
	 *     <li><b>{@code Long}</b> - Redis Integer</li>
	 *     <li><b>{@code byte[]}</b> - Redis Bulk String</li>
	 *     <li><b>{@code ServerError}</b> - Redis Error</li>
	 *     <li><b>{@code null}</b> - Redis Nil</li>
	 *     <li><b>{@code Object[]}</b> - Redis Array that may contain any type listed here</li>
	 * </ul>
	 */
	public @Nullable Object readObject() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		switch (array[head++]) {
			case STRING_MARKER:
				return decodeString();
			case ERROR_MARKER:
				return serverError();
			case LONG_MARKER:
				return decodeLong();
			case BYTES_MARKER:
				return decodeBytes();
			case ARRAY_MARKER:
				return decodeArray();
			default:
				throw new MalformedDataException("Unknown RESP data type: " + array[head - 1]);
		}
	}

	/**
	 * Skips the next Redis object found in the buffer
	 */
	public void skipObject() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		switch (array[head++]) {
			case STRING_MARKER:
			case ERROR_MARKER:
				skipString();
				break;
			case LONG_MARKER:
				skipLong();
				break;
			case BYTES_MARKER:
				skipBytes();
				break;
			case ARRAY_MARKER:
				skipArray();
				break;
			default:
				throw new MalformedDataException("Unknown RESP data type: " + array[head - 1]);
		}
	}

	/**
	 * Reads a Redis Simple String
	 */
	public String readString() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == STRING_MARKER) {
			head++;
			return decodeString();
		}
		throw new MalformedDataException("Expected Simple String, got " + getDataType(array[head]));
	}

	/**
	 * Reads a Redis Error
	 */
	public ServerError readError() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == ERROR_MARKER) {
			head++;
			return serverError();
		}
		throw new MalformedDataException("Expected Simple String, got " + getDataType(array[head]));
	}

	private String decodeString() throws MalformedDataException {
		for (int i = head; i < tail - 1; i++) {
			if (array[i] == CR) {
				if (ASSERT_PROTOCOL && array[i + 1] != LF) {
					throw new MalformedDataException("\\n does not follow \\r");
				}
				String string = new String(array, head, i - head, ISO_8859_1);
				head = i + 2;
				return string;
			}
		}
		throw NEED_MORE_DATA;
	}

	private void skipString() throws MalformedDataException {
		for (int i = head; i < tail - 1; i++) {
			if (array[i] == CR) {
				if (ASSERT_PROTOCOL && array[i + 1] != LF) {
					throw new MalformedDataException("\\n does not follow \\r");
				}
				head = i + 2;
				return;
			}
		}
		throw NEED_MORE_DATA;
	}

	/**
	 * Reads a Redis Bulk String as an array of bytes
	 */
	public byte @Nullable [] readBytes() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == BYTES_MARKER) {
			head++;
			return decodeBytes();
		}
		throw new MalformedDataException("Expected Bulk String, got " + getDataType(array[head]));
	}

	private byte @Nullable [] decodeBytes() throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) {
			return null;
		}
		if (tail - head < length + 2) throw NEED_MORE_DATA;
		if (ASSERT_PROTOCOL && array[head + length] != CR || array[head + length + 1] != LF) {
			throw new MalformedDataException("Bulk String does not end with \\r\\n");
		}
		byte[] result = new byte[length];
		System.arraycopy(array, head, result, 0, length);
		head += length + 2;
		return result;
	}

	/**
	 * Reads a Redis Bulk String as a {@link String} of a provided charset
	 *
	 * @param charset charset for encoding a string
	 */
	public @Nullable String readBytes(Charset charset) throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == BYTES_MARKER) {
			head++;
			return decodeBytes(charset);
		}
		throw new MalformedDataException("Expected Bulk String, got " + getDataType(array[head]));
	}

	private @Nullable String decodeBytes(Charset charset) throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) {
			return null;
		}
		if (tail - head < length + 2) throw NEED_MORE_DATA;
		if (ASSERT_PROTOCOL && array[head + length] != CR || array[head + length + 1] != LF) {
			throw new MalformedDataException("Bulk String does not end with \\r\\n");
		}
		String result = new String(array, head, length, charset);
		head += length + 2;
		return result;
	}

	private void skipBytes() throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) {
			return;
		}
		if (tail - head < length + 2) throw NEED_MORE_DATA;
		if (ASSERT_PROTOCOL && array[head + length] != CR || array[head + length + 1] != LF) {
			throw new MalformedDataException("Bulk String does not end with \\r\\n");
		}
		head += length + 2;
	}

	/**
	 * Reads a Redis Array
	 * <p>
	 * Array may contain any of:
	 * <ul>
	 *     <li><b>{@code String}</b> - Redis Simple String</li>
	 *     <li><b>{@code Long}</b> - Redis Integer</li>
	 *     <li><b>{@code byte[]}</b> - Redis Bulk String</li>
	 *     <li><b>{@code ServerError}</b> - Redis Error</li>
	 *     <li><b>{@code null}</b> - Redis Nil</li>
	 *     <li><b>{@code Object[]}</b> - Redis Array that may contain any type listed here</li>
	 * </ul>
	 */
	public @Nullable Object @Nullable [] readObjectArray() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == ARRAY_MARKER) {
			head++;
			return decodeArray();
		}
		throw new MalformedDataException("Expected Array, got " + getDataType(array[head]));
	}

	private @Nullable Object @Nullable [] decodeArray() throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) return null;
		Object[] result = new Object[length];
		for (int i = 0; i < length; i++) {
			result[i] = readObject();
		}
		return result;
	}

	private void skipArray() throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) return;
		for (int i = 0; i < length; i++) {
			skipObject();
		}
	}

	/**
	 * Reads a Redis Integer as Long
	 */
	public long readLong() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == LONG_MARKER) {
			head++;
			return decodeLong();
		}
		throw new MalformedDataException("Expected Integer, got " + getDataType(array[head]));
	}

	private long decodeLong() throws MalformedDataException {
		int i = head;
		long negate = 0;
		if (i != tail && array[i] == '-') {
			negate = 1;
			i++;
		}
		long result = 0;
		for (; i < tail - 1; i++) {
			if (array[i] == CR) {
				if (ASSERT_PROTOCOL && array[i + 1] != LF) {
					throw new MalformedDataException("\\n does not follow \\r");
				}
				head = i + 2;
				return (result ^ -negate) + negate;
			}
			if (ASSERT_PROTOCOL && (array[i] < '0' || array[i] > '9')) {
				throw new MalformedDataException("Malformed Integer: " + array[i]);
			}
			result = result * 10 + (array[i] - '0');
		}
		throw NEED_MORE_DATA;
	}

	private void skipLong() throws MalformedDataException {
		int i = head;
		if (i != tail && array[i] == '-') {
			i++;
		}
		for (; i < tail - 1; i++) {
			if (array[i] == CR) {
				if (ASSERT_PROTOCOL && array[i + 1] != LF) {
					throw new MalformedDataException("\\n does not follow \\r");
				}
				head = i;
				return;
			}
			if (ASSERT_PROTOCOL && (array[i] < '0' || array[i] > '9')) {
				throw new MalformedDataException("Malformed Integer: " + array[i]);
			}
		}
		throw NEED_MORE_DATA;
	}

	/**
	 * Reads a Redis Simple String 'OK'
	 */
	public void readOk() throws MalformedDataException {
		if (tail - head < 5) throw NEED_MORE_DATA;

		try {
			if (ASSERT_PROTOCOL &&
					(array[head] != STRING_MARKER ||
							array[head + 1] != 'O' || array[head + 2] != 'K' ||
							array[head + 3] != CR || array[head + 4] != LF)) {
				throw new MalformedDataException("Simple String 'OK' expected, got: " + new String(array, head, 5, ISO_8859_1));
			}
			head += 5;
		} catch (IndexOutOfBoundsException e) {
			throw NEED_MORE_DATA;
		}
	}

	void readQueued() throws MalformedDataException {
		if (tail - head < 9) throw NEED_MORE_DATA;

		try {
			if (ASSERT_PROTOCOL &&
					(array[head] != STRING_MARKER ||
							array[head + 1] != 'Q' ||
							array[head + 2] != 'U' ||
							array[head + 3] != 'E' ||
							array[head + 4] != 'U' ||
							array[head + 5] != 'E' ||
							array[head + 6] != 'D' ||
							array[head + 7] != CR || array[head + 8] != LF)) {
				throw new MalformedDataException("Simple String 'QUEUED' expected, got: " + new String(array, head, 9, ISO_8859_1));
			}
			head += 9;
		} catch (IndexOutOfBoundsException e) {
			throw NEED_MORE_DATA;
		}
	}

	long readArraySize() throws MalformedDataException {
		if (!canRead()) throw NEED_MORE_DATA;

		if (!ASSERT_PROTOCOL || array[head] == ARRAY_MARKER) {
			head++;
			return decodeLong();
		}
		throw new MalformedDataException("Expected Array, got " + getDataType(array[head]));
	}

	private static String getDataType(byte firstByte) {
		switch (firstByte) {
			case STRING_MARKER:
				return "Simple String";
			case ERROR_MARKER:
				return "Error";
			case LONG_MARKER:
				return "Integer";
			case BYTES_MARKER:
				return "Bulk String";
			case ARRAY_MARKER:
				return "Array";
			default:
				return String.format("Unknown first byte: 0x%02X", firstByte);
		}
	}

	private ServerError serverError() throws MalformedDataException {
		String errorMessage = decodeString();
		if (errorMessage.startsWith(NO_AUTH_PREFIX) || errorMessage.startsWith(ERR_AUTH_PREFIX)) {
			return new RedisAuthenticationException(errorMessage);
		}
		if (errorMessage.startsWith(NO_PERM_PREFIX)) {
			return new RedisPermissionException(errorMessage);
		}
		return new ServerError(errorMessage);
	}

}
