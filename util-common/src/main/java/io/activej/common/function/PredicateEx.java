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

package io.activej.common.function;

import io.activej.common.exception.FatalErrorHandler;
import io.activej.common.exception.UncheckedException;

import java.util.function.Predicate;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * Represents a {@link Predicate} capable of throwing exceptions
 */
@FunctionalInterface
public interface PredicateEx<T> {
	boolean test(T t) throws Exception;

	static <T> PredicateEx<T> of(Predicate<T> uncheckedFn) {
		return t -> {
			try {
				return uncheckedFn.test(t);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link Predicate} out of {@code PredicateEx}
	 * <p>
	 * If given supplier throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code PredicateEx}
	 * @return a predicate
	 */
	static <T> Predicate<T> uncheckedOf(PredicateEx<T> checkedFn) {
		return t -> {
			try {
				return checkedFn.test(t);
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
