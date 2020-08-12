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

package io.activej.serializer;

import io.activej.serializer.util.BinaryOutputUtils;

public final class BinaryOutput {
	private final byte[] array;
	private int pos;

	public BinaryOutput(byte[] array) {
		this.array = array;
	}

	public BinaryOutput(byte[] array, int pos) {
		this.array = array;
		this.pos = pos;
	}

	public byte[] array() {
		return array;
	}

	public int pos() {
		return pos;
	}

	public void pos(int pos) {
		this.pos = pos;
	}

	public void move(int delta) {
		this.pos += delta;
	}

	public void write(byte[] bytes) {
		pos = BinaryOutputUtils.write(array, pos, bytes);
	}

	public void write(byte[] bytes, int bytesOff, int len) {
		pos = BinaryOutputUtils.write(array, pos, bytes, bytesOff, len);
	}

	public void writeBoolean(boolean v) {
		pos = BinaryOutputUtils.writeBoolean(array, pos, v);
	}

	public void writeByte(byte v) {
		pos = BinaryOutputUtils.writeByte(array, pos, v);
	}

	public void writeShort(short v) {
		pos = BinaryOutputUtils.writeShort(array, pos, v);
	}

	public void writeChar(char v) {
		pos = BinaryOutputUtils.writeChar(array, pos, v);
	}

	public void writeInt(int v) {
		pos = BinaryOutputUtils.writeInt(array, pos, v);
	}

	public void writeLong(long v) {
		pos = BinaryOutputUtils.writeLong(array, pos, v);
	}

	public void writeVarInt(int v) {
		pos = BinaryOutputUtils.writeVarInt(array, pos, v);
	}

	public void writeVarLong(long v) {
		pos = BinaryOutputUtils.writeVarLong(array, pos, v);
	}

	public void writeFloat(float v) {
		pos = BinaryOutputUtils.writeFloat(array, pos, v);
	}

	public void writeDouble(double v) {
		pos = BinaryOutputUtils.writeDouble(array, pos, v);
	}

	public void writeIso88591(String s) {
		pos = BinaryOutputUtils.writeIso88591(array, pos, s);
	}

	public void writeIso88591Nullable(String s) {
		pos = BinaryOutputUtils.writeIso88591Nullable(array, pos, s);
	}

	public void writeUTF8(String s) {
		pos = BinaryOutputUtils.writeUTF8(array, pos, s);
	}

	public void writeUTF8Nullable(String s) {
		pos = BinaryOutputUtils.writeUTF8Nullable(array, pos, s);
	}

	@SuppressWarnings("deprecation")
	public void writeUTF8mb3(String s) {
		pos = BinaryOutputUtils.writeUTF8mb3(array, pos, s);
	}

	@SuppressWarnings("deprecation")
	public void writeUTF8mb3Nullable(String s) {
		pos = BinaryOutputUtils.writeUTF8mb3Nullable(array, pos, s);
	}

	public void writeUTF16(String s) {
		pos = BinaryOutputUtils.writeUTF16(array, pos, s);
	}

	public void writeUTF16LE(String s) {
		pos = BinaryOutputUtils.writeUTF16LE(array, pos, s);
	}

	public void writeUTF16Nullable(String s) {
		pos = BinaryOutputUtils.writeUTF16Nullable(array, pos, s);
	}

	public void writeUTF16NullableLE(String s) {
		pos = BinaryOutputUtils.writeUTF16NullableLE(array, pos, s);
	}

}
