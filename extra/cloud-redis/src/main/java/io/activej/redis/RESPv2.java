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

import io.activej.common.exception.MalformedDataException;

import java.nio.BufferUnderflowException;
import java.nio.charset.Charset;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

public final class RESPv2 {
	public static final byte STRING_MARKER = '+';
	public static final byte ERROR_MARKER = '-';
	public static final byte LONG_MARKER = ':';
	public static final byte BYTES_MARKER = '$';
	public static final byte ARRAY_MARKER = '*';

	private final byte[] array;
	private int head;
	private final int tail;

	public RESPv2(byte[] array, int head, int tail) {
		this.array = array;
		this.head = head;
		this.tail = tail;
	}

	public byte[] array() {
		return array;
	}

	public int head() {
		return head;
	}

	public void head(int head) {
		this.head = head;
	}

	public void moveHead(int delta) {
		this.head += delta;
	}

	public int tail() {
		return tail;
	}

	public int readRemaining() {
		return head - tail;
	}

	public boolean canRead() {
		return head < tail;
	}

	public Object readObject() throws MalformedDataException {
		switch (array[head++]) {
			case STRING_MARKER:
				return decodeString();
			case ERROR_MARKER:
				return new ServerError(decodeString());
			case LONG_MARKER:
				return decodeLong();
			case BYTES_MARKER:
				return decodeBytes();
			case ARRAY_MARKER:
				return decodeArray();
			default:
				throw new MalformedDataException();
		}
	}

	public void skipObject() throws MalformedDataException {
		switch (array[head++]) {
			case STRING_MARKER:
				skipString();
				break;
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
				throw new MalformedDataException();
		}
	}

	public String readString() throws MalformedDataException {
		if (array[head++] == STRING_MARKER) {
			return decodeString();
		}
		throw new MalformedDataException();
	}

	public String decodeString() throws MalformedDataException {
		for (int i = head; i < tail - 1; i++) {
			if (array[i] == CR)
				if (array[i + 1] == LF) {
					head = i;
					return new String(array, head, i, ISO_8859_1);
				} else {
					throw new MalformedDataException();
				}
		}
		throw new BufferUnderflowException();
	}

	private void skipString() throws MalformedDataException {
		for (int i = head; i < tail - 1; i++) {
			if (array[i] == CR)
				if (array[i + 1] == LF) {
					head = i;
					return;
				} else {
					throw new MalformedDataException();
				}
		}
		throw new BufferUnderflowException();
	}

	public byte[] readBytes() throws MalformedDataException {
		if (array[head++] == BYTES_MARKER) {
			return decodeBytes();
		}
		throw new MalformedDataException();
	}

	public byte[] decodeBytes() throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) {
			return null;
		}
		if (tail - head < length) throw new BufferUnderflowException();
		byte[] result = new byte[length];
		System.arraycopy(array, head, result, 0, length);
		head += length;
		return result;
	}

	public String readBytes(Charset charset) throws MalformedDataException {
		if (array[head++] == BYTES_MARKER) {
			return decodeBytes(charset);
		}
		throw new MalformedDataException();
	}

	public String decodeBytes(Charset charset) throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) {
			return null;
		}
		if (tail - head < length) throw new BufferUnderflowException();
		String result = new String(array, head, length, charset);
		head += length;
		return result;
	}

	private void skipBytes() throws MalformedDataException {
		int length = (int) decodeLong();
		if (length == -1) {
			return;
		}
		if (tail - head < length) throw new BufferUnderflowException();
		head += length;
	}

	public Object[] parseObjectArray() throws MalformedDataException {
		if (array[head++] == ARRAY_MARKER) {
			return decodeArray();
		}
		throw new MalformedDataException();
	}

	public Object[] decodeArray() throws MalformedDataException {
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

	public long readLong() throws MalformedDataException {
		if (array[head++] == LONG_MARKER) {
			return decodeLong();
		}
		throw new MalformedDataException();
	}

	public long decodeLong() throws MalformedDataException {
		int i = head;
		long negate = 0;
		if (i != tail && array[i] == '-') {
			negate = 1;
			i++;
		}
		long result = 0;
		for (; i < tail - 1; i++) {
			if (array[i] >= '0' && array[i] <= '9') {
				result = result * 10 + (array[i] - '0');
				continue;
			}
			if (array[i] == CR && array[i + 1] == LF) {
				head = i;
				return (result ^ -negate) + negate;
			}
			throw new MalformedDataException();
		}
		throw new BufferUnderflowException();
	}

	private void skipLong() throws MalformedDataException {
		int i = head;
		if (i != tail && array[i] == '-') {
			i++;
		}
		for (; i < tail - 1; i++) {
			if (array[i] >= '0' && array[i] <= '9') {
				continue;
			}
			if (array[i] == CR && array[i + 1] == LF) {
				head = i;
				return;
			}
			throw new MalformedDataException();
		}
		throw new BufferUnderflowException();
	}

	public void readOk() throws MalformedDataException {
		try {
			if (array[head] == STRING_MARKER &&
					array[head + 1] == CR && array[head + 2] == LF &&
					array[head + 3] == 'O' && array[head + 4] == 'K') {
				head += 5;
				return;
			}
		} catch (IndexOutOfBoundsException e) {
			throw new BufferUnderflowException();
		}
		throw new MalformedDataException();
	}

}
