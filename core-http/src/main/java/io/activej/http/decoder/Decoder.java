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

package io.activej.http.decoder;

import io.activej.common.collection.Either;
import io.activej.common.tuple.*;
import io.activej.http.HttpRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A high-level API that allows declarative definition of HTTP decoders
 * that can convert incoming requests to concrete objects.
 * This allows complex decoders to be algebraically built from simple ones.
 */
public interface Decoder<T> {
	/**
	 * Either return the decoded type or format
	 */
	Either<T, DecodeErrors> decode(@NotNull HttpRequest request);

	@Nullable
	default T decodeOrNull(@NotNull HttpRequest request) {
		return decode(request).getLeftOrNull();
	}

	default T decodeOrThrow(@NotNull HttpRequest request) throws DecodeException {
		Either<T, DecodeErrors> either = decode(request);
		if (either.isLeft()) return either.getLeft();
		throw new DecodeException(either.getRight());
	}

	/**
	 * An id that is going to be used in the error-tree if at some point the whole decoder fails
	 */
	String getId();

	default Decoder<T> withId(String id) {
		return new Decoder<T>() {
			@Override
			public Either<T, DecodeErrors> decode(@NotNull HttpRequest request) {
				return Decoder.this.decode(request);
			}

			@Override
			public String getId() {
				return id;
			}
		};
	}

	default <V> Decoder<V> map(Function<T, V> fn) {
		return mapEx(Mapper.of(fn));
	}

	default <V> Decoder<V> map(Function<T, V> fn, String message) {
		return mapEx(Mapper.of(fn, message));
	}

	/**
	 * Enhanced functional 'map' operation.
	 * If mapped returns an errors, then the returned decoder fails with that error.
	 */
	default <V> Decoder<V> mapEx(Mapper<T, V> fn) {
		return new AbstractDecoder<V>(getId()) {
			@Override
			public Either<V, DecodeErrors> decode(@NotNull HttpRequest request) {
				return Decoder.this.decode(request)
						.flatMapLeft(value ->
								fn.map(value)
										.mapRight(DecodeErrors::of));
			}
		};
	}

	default Decoder<T> validate(Predicate<T> predicate, String error) {
		return validate(Validator.of(predicate, error));
	}

	/**
	 * Enhanced functional 'filter' operation.
	 * If validator returns non-empty list of errors,
	 * then the returned decoder fails with these errors.
	 */
	default Decoder<T> validate(Validator<T> validator) {
		return new AbstractDecoder<T>(getId()) {
			@Override
			public Either<T, DecodeErrors> decode(@NotNull HttpRequest request) {
				Either<T, DecodeErrors> decodedValue = Decoder.this.decode(request);
				if (decodedValue.isRight()) return decodedValue;
				List<DecodeError> errors = validator.validate(decodedValue.getLeft());
				if (errors.isEmpty()) return decodedValue;
				return Either.right(DecodeErrors.of(errors));
			}
		};
	}

	@NotNull
	static <V> Decoder<V> create(Function<Object[], V> constructor, String message, Decoder<?>... decoders) {
		return createEx(Mapper.of(constructor, message), decoders);
	}

	@NotNull
	static <V> Decoder<V> create(Function<Object[], V> constructor, Decoder<?>... decoders) {
		return createEx(Mapper.of(constructor), decoders);
	}

	/**
	 * Plainly combines given decoders (they are called on the same request) into one, mapping the result
	 * with the supplied mapper.
	 */
	@NotNull
	static <V> Decoder<V> createEx(Mapper<Object[], V> constructor, Decoder<?>... decoders) {
		return new AbstractDecoder<V>("") {
			@Override
			public Either<V, DecodeErrors> decode(@NotNull HttpRequest request) {
				Object[] args = new Object[decoders.length];
				DecodeErrors errors = DecodeErrors.create();
				for (int i = 0; i < decoders.length; i++) {
					Decoder<?> decoder = decoders[i];
					Either<?, DecodeErrors> decoded = decoder.decode(request);
					if (decoded.isLeft()) {
						args[i] = decoded.getLeft();
					} else {
						errors.with(decoder.getId(), decoded.getRight());
					}
				}
				if (errors.hasErrors()) {
					return Either.right(errors);
				}
				return constructor.map(args)
						.mapRight(DecodeErrors::of);
			}
		};
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1> Decoder<R> of(TupleConstructor1<T1, R> constructor, Decoder<T1> param1) {
		return create(params -> constructor.create((T1) params[0]),
				param1);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2> Decoder<R> of(TupleConstructor2<T1, T2, R> constructor,
			Decoder<T1> param1,
			Decoder<T2> param2) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1]),
				param1, param2);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3> Decoder<R> of(TupleConstructor3<T1, T2, T3, R> constructor,
			Decoder<T1> param1,
			Decoder<T2> param2,
			Decoder<T3> param3) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2]),
				param1, param2, param3);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4> Decoder<R> of(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			Decoder<T1> param1,
			Decoder<T2> param2,
			Decoder<T3> param3,
			Decoder<T4> param4) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3]),
				param1, param2, param3, param4);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4, T5> Decoder<R> of(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			Decoder<T1> param1,
			Decoder<T2> param2,
			Decoder<T3> param3,
			Decoder<T4> param4,
			Decoder<T5> param5) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3], (T5) params[4]),
				param1, param2, param3, param4, param5);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4, T5, T6> Decoder<R> of(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Decoder<T1> param1,
			Decoder<T2> param2,
			Decoder<T3> param3,
			Decoder<T4> param4,
			Decoder<T5> param5,
			Decoder<T6> param6) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3], (T5) params[5], (T6) params[6]),
				param1, param2, param3, param4, param5, param6);
	}
}

