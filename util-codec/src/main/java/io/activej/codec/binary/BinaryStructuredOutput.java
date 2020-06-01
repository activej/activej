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
import io.activej.bytebuf.ByteBufPool;
import io.activej.codec.StructuredEncoder;
import io.activej.codec.StructuredOutput;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class BinaryStructuredOutput implements StructuredOutput {
	private ByteBuf buf = ByteBufPool.allocate(256);

	public ByteBuf getBuf() {
		return buf;
	}

	@Override
	public void writeBoolean(boolean value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 1);
		buf.writeBoolean(value);
	}

	@Override
	public void writeByte(byte value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 1);
		buf.writeByte(value);
	}

	@Override
	public void writeInt(int value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 5);
		buf.writeVarInt(value);
	}

	@Override
	public void writeLong(long value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 9);
		buf.writeVarLong(value);
	}

	@Override
	public void writeInt32(int value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 4);
		buf.writeInt(value);
	}

	@Override
	public void writeLong64(long value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 8);
		buf.writeLong(value);
	}

	@Override
	public void writeFloat(float value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 4);
		buf.writeFloat(value);
	}

	@Override
	public void writeDouble(double value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 8);
		buf.writeDouble(value);
	}

	@Override
	public void writeBytes(byte[] bytes, int off, int len) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 5 + len);
		buf.writeVarInt(bytes.length);
		buf.write(bytes, off, len);
	}

	@Override
	public void writeString(String value) {
		buf = ByteBufPool.ensureWriteRemaining(buf, 5 + value.length() * 5);
		byte[] bytes = value.getBytes(UTF_8);
		buf.writeVarInt(bytes.length);
		buf.write(bytes);
	}

	@Override
	public void writeNull() {
		writeBoolean(false);
	}

	@Override
	public <T> void writeNullable(StructuredEncoder<T> encoder, T value) {
		if (value != null) {
			writeBoolean(true);
			encoder.encode(this, value);
		} else {
			writeBoolean(false);
		}
	}

	@Override
	public <T> void writeList(StructuredEncoder<T> encoder, List<T> list) {
		writeInt(list.size());
		for (T item : list) {
			encoder.encode(this, item);
		}
	}

	@Override
	public <K, V> void writeMap(StructuredEncoder<K> keyEncoder, StructuredEncoder<V> valueEncoder, Map<K, V> map) {
		writeInt(map.size());
		for (Map.Entry<K, V> entry : map.entrySet()) {
			keyEncoder.encode(this, entry.getKey());
			valueEncoder.encode(this, entry.getValue());
		}
	}

	@Override
	public <T> void writeTuple(StructuredEncoder<T> encoder, T value) {
		encoder.encode(this, value);
	}

	@Override
	public <T> void writeObject(StructuredEncoder<T> encoder, T value) {
		encoder.encode(this, value);
	}

	@Override
	public void writeKey(String field) {
		writeString(field);
	}

	@Override
	public <T> void writeCustom(Type type, T value) {
		throw new UnsupportedOperationException("No custom type writers");
	}
}
