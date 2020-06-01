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

package io.activej.common.parse;

import io.activej.common.exception.UncheckedException;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

@FunctionalInterface
public interface ParserFunction<T, R> {
	R parse(T value) throws ParseException;

	static <T, R> Function<T, R> asFunction(ParserFunction<T, R> fn) {
		return item -> {
			try {
				return fn.parse(item);
			} catch (ParseException e) {
				throw new UncheckedException(e);
			}
		};
	}

	static <T, R> ParserFunction<T, R> of(Function<T, R> fn) {
		return value -> {
			try {
				return fn.apply(value);
			} catch (Exception e) {
				throw new ParseException(e);
			}
		};
	}

	default R parseOrDefault(@Nullable T value, R defaultResult) {
		try {
			if (value != null) {
				return parse(value);
			}
		} catch (ParseException ignore) {}

		return defaultResult;
	}

	default <V> ParserFunction<T, V> andThen(ParserFunction<? super R, ? extends V> after) {
		return (T t) -> after.parse(parse(t));
	}
}
