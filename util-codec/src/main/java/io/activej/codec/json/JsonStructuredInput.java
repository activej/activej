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

package io.activej.codec.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.MalformedJsonException;
import io.activej.codec.StructuredDecoder;
import io.activej.codec.StructuredInput;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UncheckedException;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static io.activej.codec.StructuredCodecs.STRING_CODEC;
import static io.activej.codec.StructuredInput.Token.*;

public final class JsonStructuredInput implements StructuredInput {
	private final JsonReader reader;

	/**
	 * Constructs a new {@link JsonStructuredInput}
	 * Passed {@link JsonReader} should not perform any blocking I/O operations
	 *
	 * @param reader nonblocking {@link JsonReader}
	 */
	public JsonStructuredInput(JsonReader reader) {
		this.reader = reader;
	}

	@Override
	public boolean readBoolean() throws MalformedDataException {
		try {
			return reader.nextBoolean();
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public byte readByte() throws MalformedDataException {
		int n = readInt();
		if (n != (n & 0xFF)) throw new MalformedDataException("Expected byte, but was: " + n);
		return (byte) n;
	}

	@Override
	public int readInt() throws MalformedDataException {
		try {
			return reader.nextInt();
		} catch (EOFException | MalformedJsonException | NumberFormatException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public long readLong() throws MalformedDataException {
		try {
			return reader.nextLong();
		} catch (EOFException | MalformedJsonException | NumberFormatException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public int readInt32() throws MalformedDataException {
		return readInt();
	}

	@Override
	public long readLong64() throws MalformedDataException {
		return readLong();
	}

	@Override
	public float readFloat() throws MalformedDataException {
		return (float) readDouble();
	}

	@Override
	public double readDouble() throws MalformedDataException {
		try {
			return reader.nextDouble();
		} catch (EOFException | MalformedJsonException | NumberFormatException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public byte[] readBytes() throws MalformedDataException {
		try {
			return Base64.getDecoder().decode(readString());
		} catch (IllegalArgumentException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public String readString() throws MalformedDataException {
		try {
			return reader.nextString();
		} catch (EOFException | NumberFormatException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void readNull() throws MalformedDataException {
		try {
			reader.nextNull();
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readNullable(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			if (reader.peek() == JsonToken.NULL) {
				reader.nextNull();
				return null;
			}
			return decoder.decode(this);
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readTuple(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			reader.beginArray();
			T result = decoder.decode(this);
			reader.endArray();
			return result;
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readObject(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			reader.beginObject();
			T result = decoder.decode(this);
			reader.endObject();
			return result;
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> List<T> readList(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			List<T> list = new ArrayList<>();
			reader.beginArray();
			while (reader.hasNext()) {
				T item = decoder.decode(this);
				list.add(item);
			}
			reader.endArray();
			return list;
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws MalformedDataException {
		try {
			Map<K, V> map = new LinkedHashMap<>();
			if (keyDecoder == STRING_CODEC) {
				reader.beginObject();
				while (reader.hasNext()) {
					K key = (K) reader.nextName();
					V value = valueDecoder.decode(this);
					map.put(key, value);
				}
				reader.endObject();
			} else {
				reader.beginArray();
				while (reader.hasNext()) {
					reader.beginArray();
					K key = keyDecoder.decode(this);
					V value = valueDecoder.decode(this);
					map.put(key, value);
					reader.endArray();
				}
				reader.endArray();
			}
			return map;
		} catch (EOFException | NumberFormatException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public boolean hasNext() throws MalformedDataException {
		try {
			JsonToken token = reader.peek();
			return token != JsonToken.END_ARRAY && token != JsonToken.END_OBJECT;
		} catch (EOFException | MalformedJsonException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public String readKey() throws MalformedDataException {
		try {
			return reader.nextName();
		} catch (EOFException | NumberFormatException | MalformedJsonException | IllegalStateException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	@SuppressWarnings("RedundantThrows")
	public <T> T readCustom(Type type) throws MalformedDataException {
		throw new UnsupportedOperationException("No custom type readers");
	}

	@Override
	public EnumSet<Token> getNext() throws MalformedDataException {
		JsonToken jsonToken;
		try {
			jsonToken = reader.peek();
		} catch (EOFException | MalformedJsonException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}

		switch (jsonToken) {
			case NULL:
				return EnumSet.of(NULL);
			case BOOLEAN:
				return EnumSet.of(BOOLEAN);
			case NUMBER:
				return EnumSet.of(BYTE, INT, LONG, FLOAT, DOUBLE);
			case STRING:
				return EnumSet.of(STRING, BYTES);
			case BEGIN_ARRAY:
				return EnumSet.of(LIST, TUPLE);
			case BEGIN_OBJECT:
				return EnumSet.of(MAP, OBJECT);
			default:
				throw new MalformedDataException("Invalid token: " + jsonToken);
		}
	}

}
