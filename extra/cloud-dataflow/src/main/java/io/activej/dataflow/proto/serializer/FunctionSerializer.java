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

package io.activej.dataflow.proto.serializer;

import com.google.protobuf.ByteString;
import io.activej.common.exception.MalformedDataException;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.common.Checks.checkNotNull;

public final class FunctionSerializer {
	private @Nullable BinarySerializer<Function<?, ?>> functionSerializer;
	private @Nullable BinarySerializer<Predicate<?>> predicateSerializer;
	private @Nullable BinarySerializer<Comparator<?>> comparatorSerializer;
	private @Nullable BinarySerializer<Reducer<?, ?, ?, ?>> reducerSerializer;
	private @Nullable BinarySerializer<Joiner<?, ?, ?, ?>> joinerSerializer;

	public void setFunctionSerializer(@NotNull BinarySerializer<Function<?, ?>> functionSerializer) {
		this.functionSerializer = functionSerializer;
	}

	public void setPredicateSerializer(@NotNull BinarySerializer<Predicate<?>> predicateSerializer) {
		this.predicateSerializer = predicateSerializer;
	}

	public void setComparatorSerializer(@NotNull BinarySerializer<Comparator<?>> comparatorSerializer) {
		this.comparatorSerializer = comparatorSerializer;
	}

	public void setReducerSerializer(@NotNull BinarySerializer<Reducer<?, ?, ?, ?>> reducerSerializer) {
		this.reducerSerializer = reducerSerializer;
	}

	public void setJoinerSerializer(@NotNull BinarySerializer<Joiner<?, ?, ?, ?>> joinerSerializer) {
		this.joinerSerializer = joinerSerializer;
	}

	// region serializers
	public ByteString serializeFunction(Function<?, ?> function) {
		return doSerialize(checkNotNull(functionSerializer), function);
	}

	public ByteString serializePredicate(Predicate<?> predicate) {
		return doSerialize(checkNotNull(predicateSerializer), predicate);
	}

	public ByteString serializeComparator(Comparator<?> comparator) {
		return doSerialize(checkNotNull(comparatorSerializer), comparator);
	}

	public ByteString serializeReducer(Reducer<?, ?, ?, ?> reducer) {
		return doSerialize(checkNotNull(reducerSerializer), reducer);
	}

	public ByteString serializeJoiner(Joiner<?, ?, ?, ?> joiner) {
		return doSerialize(checkNotNull(joinerSerializer), joiner);
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
		return doDeserialize(checkNotNull(functionSerializer), byteString);
	}

	public Predicate<?> deserializePredicate(ByteString byteString) throws MalformedDataException {
		return doDeserialize(checkNotNull(predicateSerializer), byteString);
	}

	public Comparator<?> deserializeComparator(ByteString byteString) throws MalformedDataException {
		return doDeserialize(checkNotNull(comparatorSerializer), byteString);
	}

	public Reducer<?, ?, ?, ?> deserializeReducer(ByteString byteString) throws MalformedDataException {
		return doDeserialize(checkNotNull(reducerSerializer), byteString);
	}

	public Joiner<?, ?, ?, ?> deserializeJoiner(ByteString byteString) throws MalformedDataException {
		return doDeserialize(checkNotNull(joinerSerializer), byteString);
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
