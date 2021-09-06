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

import java.util.function.BiFunction;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * Represents a {@link BiFunction} capable of throwing exceptions
 */
@FunctionalInterface
public interface BiFunctionEx<T, U, R> {
	R apply(T t, U u) throws Exception;

	/**
	 * Creates a {@code BiFunctionEx} out of {@link BiFunction}
	 * <p>
	 * If given function throws {@link UncheckedException}, its cause will be propagated
	 *
	 * @param uncheckedFn original {@link BiFunction}
	 * @return a function capable of throwing exceptions
	 */
	static <T, U, R> BiFunctionEx<T, U, R> of(BiFunction<T, U, R> uncheckedFn) {
		return (t, u) -> {
			try {
				return uncheckedFn.apply(t, u);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link BiFunction} out of {@code BiFunctionEx}
	 * <p>
	 * If given function throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code BiFunctionEx}
	 * @return a function
	 */
	static <T, U, R> BiFunction<T, U, R> uncheckedOf(BiFunctionEx<T, U, R> checkedFn) {
		return (t, u) -> {
			try {
				return checkedFn.apply(t, u);
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
