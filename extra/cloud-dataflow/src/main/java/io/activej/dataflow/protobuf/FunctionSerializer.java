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

package io.activej.dataflow.protobuf;

import com.google.protobuf.ByteString;
import io.activej.common.exception.MalformedDataException;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

public final class FunctionSerializer {
	private final BinarySerializer<Function<?, ?>> functionSerializer;
	private final BinarySerializer<Predicate<?>> predicateSerializer;
	private final BinarySerializer<Comparator<?>> comparatorSerializer;
	private final BinarySerializer<Reducer<?, ?, ?, ?>> reducerSerializer;
	private final BinarySerializer<Joiner<?, ?, ?, ?>> joinerSerializer;

	public FunctionSerializer(
			BinarySerializer<Function<?, ?>> functionSerializer,
			BinarySerializer<Predicate<?>> predicateSerializer,
			BinarySerializer<Comparator<?>> comparatorSerializer,
			BinarySerializer<Reducer<?, ?, ?, ?>> reducerSerializer,
			BinarySerializer<Joiner<?, ?, ?, ?>> joinerSerializer
	) {
		this.functionSerializer = functionSerializer;
		this.predicateSerializer = predicateSerializer;
		this.comparatorSerializer = comparatorSerializer;
		this.reducerSerializer = reducerSerializer;
		this.joinerSerializer = joinerSerializer;
	}

	// region serializers
	public ByteString serializeFunction(Function<?, ?> function) {
		return doSerialize(functionSerializer, function);
	}

	public ByteString serializePredicate(Predicate<?> predicate) {
		return doSerialize(predicateSerializer, predicate);
	}

	public ByteString serializeComparator(Comparator<?> comparator) {
		return doSerialize(comparatorSerializer, comparator);
	}

	public ByteString serializeReducer(Reducer<?, ?, ?, ?> reducer) {
		return doSerialize(reducerSerializer, reducer);
	}

	public ByteString serializeJoiner(Joiner<?, ?, ?, ?> joiner) {
		return doSerialize(joinerSerializer, joiner);
	}

	private static <T> ByteString doSerialize(BinarySerializer<T> serializer, T item) {
		int length = 100;
		while (true) {
			byte[] bytes = new byte[length];
			try {
				int encoded = serializer.encode(bytes, 0, item);
				return ByteString.copyFrom(bytes, 0, encoded);
			} catch (IndexOutOfBoundsException e) {
				length *= 2;
			}
		}
	}
	// endregion

	// region deserializers
	public Function<?, ?> deserializeFunction(ByteString byteString) throws MalformedDataException {
		return doDeserialize(functionSerializer, byteString);
	}

	public Predicate<?> deserializePredicate(ByteString byteString) throws MalformedDataException {
		return doDeserialize(predicateSerializer, byteString);
	}

	public Comparator<?> deserializeComparator(ByteString byteString) throws MalformedDataException {
		return doDeserialize(comparatorSerializer, byteString);
	}

	public Reducer<?, ?, ?, ?> deserializeReducer(ByteString byteString) throws MalformedDataException {
		return doDeserialize(reducerSerializer, byteString);
	}

	public Joiner<?, ?, ?, ?> deserializeJoiner(ByteString byteString) throws MalformedDataException {
		return doDeserialize(joinerSerializer, byteString);
	}

	private static <T> T doDeserialize(BinarySerializer<T> serializer, ByteString byteString) throws MalformedDataException {
		try {
			return serializer.decode(byteString.toByteArray(), 0);
		} catch (CorruptedDataException e) {
			throw new MalformedDataException(e);
		}
	}
	// endregion
}
