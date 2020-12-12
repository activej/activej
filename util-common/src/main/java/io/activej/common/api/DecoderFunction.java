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

package io.activej.common.api;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UncheckedException;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

@FunctionalInterface
public interface DecoderFunction<T, R> {
	R decode(T value) throws MalformedDataException;

	static <T, R> Function<T, R> asFunction(DecoderFunction<T, R> fn) {
		return item -> {
			try {
				return fn.decode(item);
			} catch (MalformedDataException e) {
				throw new UncheckedException(e);
			}
		};
	}

	static <T, R> DecoderFunction<T, R> of(Function<T, R> fn) {
		return value -> {
			try {
				return fn.apply(value);
			} catch (Exception e) {
				throw new MalformedDataException(e);
			}
		};
	}

	default R decodeOrDefault(@Nullable T value, R defaultResult) {
		try {
			if (value != null) {
				return decode(value);
			}
		} catch (MalformedDataException ignore) {}

		return defaultResult;
	}

	default <V> DecoderFunction<T, V> andThen(DecoderFunction<? super R, ? extends V> after) {
		return (T t) -> after.decode(decode(t));
	}
}
