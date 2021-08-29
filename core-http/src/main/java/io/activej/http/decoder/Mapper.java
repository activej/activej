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
import io.activej.common.function.FunctionEx;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * An enhanced mapping function which can return a list of errors for given input object.
 * This can be used to both map and put additional constraints on the decoded object from HTTP decoder.
 * For example to ensure, that age of the person given as a string is <b>an integer</b> in range 0-100
 * and convert it to that.
 */
@FunctionalInterface
public interface Mapper<T, V> {
	Either<V, List<DecodeError>> map(T value);

	static <T, V> Mapper<T, V> of(Function<T, V> fn) {
		return value -> Either.left(fn.apply(value));
	}

	static <T, V> Mapper<T, V> ofEx(FunctionEx<T, V> fn, String message) {
		return value -> {
			try {
				return Either.left(fn.apply(value));
			} catch (Exception ex) {
				return Either.right(singletonList(DecodeError.of(message, value)));
			}
		};
	}
}
