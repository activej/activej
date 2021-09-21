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

package io.activej.async.function;

import io.activej.common.function.PredicateEx;
import io.activej.promise.Promise;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@FunctionalInterface
public interface AsyncPredicate<T> {
	Promise<Boolean> test(T t);

	/**
	 * Wraps a {@link PredicateEx} interface.
	 *
	 * @param predicate a {@link PredicateEx}
	 * @return {@link AsyncPredicate} that works on top of {@link PredicateEx} interface
	 */
	static <T> AsyncPredicate<T> of(PredicateEx<T> predicate) {
		return value -> {
			try {
				return Promise.of(predicate.test(value));
			} catch (Exception e) {
				handleError(e, predicate);
				return Promise.ofException(e);
			}
		};
	}

	static <T> AsyncPredicate<T> alwaysTrue() {
		return t -> Promise.of(Boolean.TRUE);
	}

	static <T> AsyncPredicate<T> alwaysFalse() {
		return t -> Promise.of(Boolean.FALSE);
	}

	static <T> AsyncPredicate<T> not(AsyncPredicate<? super T> predicate) {
		//noinspection unchecked
		return (AsyncPredicate<T>) predicate.negate();
	}

	default AsyncPredicate<T> negate() {
		return t -> test(t).map(b -> !b);
	}

	default AsyncPredicate<T> and(AsyncPredicate<? super T> other) {
		return t -> test(t).combine(other.test(t), (b1, b2) -> b1 && b2);
	}

	default AsyncPredicate<T> or(AsyncPredicate<? super T> other) {
		return t -> test(t).combine(other.test(t), (b1, b2) -> b1 || b2);
	}
}
