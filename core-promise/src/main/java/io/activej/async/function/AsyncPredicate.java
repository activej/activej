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

import io.activej.promise.Promise;

import java.util.function.Predicate;

@FunctionalInterface
public interface AsyncPredicate<T> {
	Promise<Boolean> test(T t);

	static <T> AsyncPredicate<T> cast(AsyncPredicate<T> predicate) {
		return predicate;
	}

	static <T> AsyncPredicate<T> of(Predicate<T> predicate) {
		return new AsyncPredicates.AsyncPredicateWrapper<>(predicate);
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

	static <T> AsyncPredicate<T> alwaysTrue() {
		return t -> Promise.of(Boolean.TRUE);
	}

	static <T> AsyncPredicate<T> alwaysFalse() {
		return t -> Promise.of(Boolean.FALSE);
	}
}
