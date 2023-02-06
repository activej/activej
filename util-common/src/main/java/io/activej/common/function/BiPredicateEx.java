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

import java.util.function.BiPredicate;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * Represents a {@link BiPredicate} capable of throwing exceptions
 */
@FunctionalInterface
public interface BiPredicateEx<T, U> {
	boolean test(T t, U u) throws Exception;

	static <T, U> BiPredicateEx<T, U> of(BiPredicate<T, U> uncheckedFn) {
		return (t, u) -> {
			try {
				return uncheckedFn.test(t, u);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link BiPredicate} out of {@code BiPredicateEx}
	 * <p>
	 * If given supplier throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code BiPredicateEx}
	 * @return a predicate
	 */
	static <T, U> BiPredicate<T, U> uncheckedOf(BiPredicateEx<T, U> checkedFn) {
		return (t, u) -> {
			try {
				return checkedFn.test(t, u);
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
