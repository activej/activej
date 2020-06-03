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

import com.google.gson.stream.JsonWriter;
import io.activej.codec.StructuredCodecs;
import io.activej.codec.StructuredEncoder;
import io.activej.codec.StructuredOutput;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class JsonStructuredOutput implements StructuredOutput {
	final JsonWriter writer;

	/**
	 * Constructs a new {@link JsonStructuredOutput}
	 * Passed {@link JsonWriter} should not perform any blocking I/O operations
	 *
	 * @param writer nonblocking {@link JsonWriter}
	 */
	public JsonStructuredOutput(JsonWriter writer) {
		this.writer = writer;
	}

	@Override
	public void writeBoolean(boolean value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeByte(byte value) {
		try {
			writer.value(value & 0xFF);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeInt(int value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeLong(long value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeInt32(int value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeLong64(long value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeFloat(float value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeDouble(double value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeBytes(byte[] bytes, int off, int len) {
		try {
			writer.value(Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes, off, off + len)));
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeBytes(byte[] bytes) {
		try {
			writer.value(Base64.getEncoder().encodeToString(bytes));
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeString(String value) {
		try {
			writer.value(value);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeNull() {
		try {
			writer.nullValue();
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> void writeNullable(StructuredEncoder<T> encoder, T value) {
		if (value != null) {
			encoder.encode(this, value);
		} else {
			writeNull();
		}
	}

	@Override
	public <T> void writeList(StructuredEncoder<T> encoder, List<T> list) {
		try {
			writer.beginArray();
			for (T item : list) {
				encoder.encode(this, item);
			}
			writer.endArray();
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public <K, V> void writeMap(StructuredEncoder<K> keyEncoder, StructuredEncoder<V> valueEncoder, Map<K, V> map) {
		try {
			if (keyEncoder == StructuredCodecs.STRING_CODEC) {
				writer.beginObject();
				for (Map.Entry<K, V> entry : map.entrySet()) {
					writer.name((String) entry.getKey());
					valueEncoder.encode(this, entry.getValue());
				}
				writer.endObject();
			} else {
				writer.beginArray();
				for (Map.Entry<K, V> entry : map.entrySet()) {
					writer.beginArray();
					keyEncoder.encode(this, entry.getKey());
					valueEncoder.encode(this, entry.getValue());
					writer.endArray();
				}
				writer.endArray();
			}
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> void writeTuple(StructuredEncoder<T> encoder, T value) {
		try {
			writer.beginArray();
			encoder.encode(this, value);
			writer.endArray();
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> void writeObject(StructuredEncoder<T> encoder, T value) {
		try {
			writer.beginObject();
			encoder.encode(this, value);
			writer.endObject();
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeKey(String field) {
		try {
			writer.name(field);
		} catch (IOException ignored) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> void writeCustom(Type type, T value) {
		throw new UnsupportedOperationException("No custom type writers");
	}
}
