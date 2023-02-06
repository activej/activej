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

import java.util.function.Predicate;

import static io.activej.common.exception.FatalErrorHandler.handleError;

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

	/**
	 * Returns an {@link AsyncPredicate} that always returns promise of {@code true}
	 */
	static <T> AsyncPredicate<T> alwaysTrue() {
		return t -> Promise.of(Boolean.TRUE);
	}

	/**
	 * Returns an {@link AsyncPredicate} that always returns promise of {@code false}
	 */
	static <T> AsyncPredicate<T> alwaysFalse() {
		return t -> Promise.of(Boolean.FALSE);
	}

	/**
	 * Negates a given {@link AsyncPredicate}
	 *
	 * @param predicate an original {@link AsyncPredicate}
	 * @return an {@link AsyncPredicate} that represents a logical negation of original predicate
	 */
	static <T> AsyncPredicate<T> not(AsyncPredicate<? super T> predicate) {
		//noinspection unchecked
		return (AsyncPredicate<T>) predicate.negate();
	}

	/**
	 * Negates {@code this} {@link AsyncPredicate}
	 *
	 * @return an {@link AsyncPredicate} that represents a logical negation of {@code this} predicate
	 */
	default AsyncPredicate<T> negate() {
		return t -> test(t).map(b -> !b);
	}

	/**
	 * Returns a composed {@link AsyncPredicate} that represents a logical AND of this
	 * asynchronous predicate and the other
	 * <p>
	 * Unlike Java's {@link Predicate#and(Predicate)} this method does not provide short-circuit.
	 * If either {@code this} or {@code other} predicate returns an exceptionally completed promise,
	 * the combined asynchronous predicate also returns an exceptionally completed promise
	 *
	 * @param other other {@link AsyncPredicate} that will be logically ANDed with {@code this} predicate
	 * @return a composed {@link AsyncPredicate} that represents a logical AND of this
	 * asynchronous predicate and the other
	 */
	default AsyncPredicate<T> and(AsyncPredicate<? super T> other) {
		return t -> test(t).combine(other.test(t), (b1, b2) -> b1 && b2);
	}

	/**
	 * Returns a composed {@link AsyncPredicate} that represents a logical OR of this
	 * asynchronous predicate and the other
	 * <p>
	 * Unlike Java's {@link Predicate#or(Predicate)} this method does not provide short-circuit.
	 * If either {@code this} or {@code other} predicate returns an exceptionally completed promise,
	 * the combined asynchronous predicate also returns an exceptionally completed promise
	 *
	 * @param other other {@link AsyncPredicate} that will be logically ORed with {@code this} predicate
	 * @return a composed {@link AsyncPredicate} that represents a logical OR of this
	 * asynchronous predicate and the other
	 */
	default AsyncPredicate<T> or(AsyncPredicate<? super T> other) {
		return t -> test(t).combine(other.test(t), (b1, b2) -> b1 || b2);
	}
}
