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

package io.activej.codec;

import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * This is an abstraction that allows you to read data in uniform way
 * from different sources with different implementations of this interface
 */
public interface StructuredInput {
	void readNull() throws MalformedDataException;

	boolean readBoolean() throws MalformedDataException;

	byte readByte() throws MalformedDataException;

	int readInt() throws MalformedDataException;

	long readLong() throws MalformedDataException;

	int readInt32() throws MalformedDataException;

	long readLong64() throws MalformedDataException;

	float readFloat() throws MalformedDataException;

	double readDouble() throws MalformedDataException;

	byte[] readBytes() throws MalformedDataException;

	String readString() throws MalformedDataException;

	@Nullable <T> T readNullable(StructuredDecoder<T> decoder) throws MalformedDataException;

	boolean hasNext() throws MalformedDataException;

	String readKey() throws MalformedDataException;

	default void readKey(String expectedName) throws MalformedDataException {
		String actualName = readKey();
		if (!expectedName.equals(actualName)) {
			throw new MalformedDataException("Expected field: " + expectedName + ", but was: " + actualName);
		}
	}

	default <T> T readKey(String expectedName, StructuredDecoder<T> decoder) throws MalformedDataException {
		readKey(expectedName);
		return decoder.decode(this);
	}

	<T> List<T> readList(StructuredDecoder<T> decoder) throws MalformedDataException;

	<K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws MalformedDataException;

	<T> T readTuple(StructuredDecoder<T> decoder) throws MalformedDataException;

	<T> T readObject(StructuredDecoder<T> decoder) throws MalformedDataException;

	@FunctionalInterface
	interface DecoderRunnable {
		void run() throws MalformedDataException;
	}

	default void readTuple(DecoderRunnable decoder) throws MalformedDataException {
		readTuple(in -> {
			decoder.run();
			return null;
		});
	}

	default void readObject(DecoderRunnable decoder) throws MalformedDataException {
		readObject(in -> {
			decoder.run();
			return null;
		});
	}

	<T> T readCustom(Type type) throws MalformedDataException;

	enum Token {
		NULL, BOOLEAN, BYTE, INT, LONG, FLOAT, DOUBLE, STRING, BYTES, LIST, MAP, TUPLE, OBJECT
	}

	EnumSet<Token> getNext() throws MalformedDataException;
}
