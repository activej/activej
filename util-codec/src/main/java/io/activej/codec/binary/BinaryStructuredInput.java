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

package io.activej.codec.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.codec.StructuredDecoder;
import io.activej.codec.StructuredInput;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UncheckedException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("RedundantThrows")
public final class BinaryStructuredInput implements StructuredInput {
	private final ByteBuf buf;

	public BinaryStructuredInput(ByteBuf buf) {
		this.buf = buf;
	}

	@Override
	public boolean readBoolean() throws MalformedDataException {
		try {
			return buf.readBoolean();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public byte readByte() throws MalformedDataException {
		try {
			return buf.readByte();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public int readInt() throws MalformedDataException {
		try {
			return buf.readVarInt();
		} catch (ArrayIndexOutOfBoundsException | IllegalStateException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public long readLong() throws MalformedDataException {
		try {
			return buf.readVarLong();
		} catch (ArrayIndexOutOfBoundsException | IllegalStateException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public int readInt32() throws MalformedDataException {
		try {
			return buf.readInt();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public long readLong64() throws MalformedDataException {
		try {
			return buf.readLong();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public float readFloat() throws MalformedDataException {
		try {
			return buf.readFloat();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public double readDouble() throws MalformedDataException {
		try {
			return buf.readDouble();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public byte[] readBytes() throws MalformedDataException {
		int length = readInt();
		if (length < 0 || length > buf.readRemaining()) {
			throw new MalformedDataException("Invalid length: " + length + ", remaining: " + buf.readRemaining() + ", buf: " + buf);
		}
		byte[] result = new byte[length];
		buf.read(result);
		return result;
	}

	@Override
	public String readString() throws MalformedDataException {
		int length = readInt();
		if (length == 0)
			return "";
		if (length > buf.readRemaining())
			throw new MalformedDataException("Read string length is greater than the remaining data");
		String result = new String(buf.array(), buf.head(), length, UTF_8);
		buf.moveHead(length);
		return result;
	}

	@Override
	public void readNull() throws MalformedDataException {
		if (readBoolean()) {
			throw new MalformedDataException("Expected NULL value");
		}
	}

	@Nullable
	@Override
	public <T> T readNullable(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			return readBoolean() ? decoder.decode(this) : null;
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}
	}

	@Override
	public <T> List<T> readList(StructuredDecoder<T> decoder) throws MalformedDataException {
		int size = readInt();
		List<T> list = new ArrayList<>(size);
		try {
			for (int i = 0; i < size; i++) {
				list.add(decoder.decode(this));
			}
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}
		return list;
	}

	@Override
	public <K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws MalformedDataException {
		int size = readInt();
		Map<K, V> map = new LinkedHashMap<>();
		try {
			for (int i = 0; i < size; i++) {
				map.put(keyDecoder.decode(this), valueDecoder.decode(this));
			}
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}
		return map;
	}

	@Override
	public <T> T readTuple(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			return decoder.decode(this);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}
	}

	@Override
	public <T> T readObject(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			return decoder.decode(this);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}
	}

	@Override
	public boolean hasNext() throws MalformedDataException {
		throw new UnsupportedOperationException("hasNext() is not supported for binary data");
	}

	@Override
	public String readKey() throws MalformedDataException {
		return readString();
	}

	@Override
	public <T> T readCustom(Type type) throws MalformedDataException {
		throw new UnsupportedOperationException("No custom type readers");
	}

	@Override
	public EnumSet<Token> getNext() throws MalformedDataException {
		throw new UnsupportedOperationException("getNext() is not supported for binary data");
	}

}
