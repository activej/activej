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

import java.util.function.Function;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * Represents a {@link Function} capable of throwing exceptions
 */
@FunctionalInterface
public interface FunctionEx<T, R> {
	R apply(T t) throws Exception;

	/**
	 * Creates a {@code FunctionEx} out of {@link Function}
	 * <p>
	 * If given function throws {@link UncheckedException}, its cause will be propagated
	 *
	 * @param fn original {@link Function}
	 * @return a function capable of throwing exceptions
	 */
	static <T, R> FunctionEx<T, R> of(Function<T, R> fn) {
		return t -> {
			try {
				return fn.apply(t);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link Function} out of {@code FunctionEx}
	 * <p>
	 * If given function throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code FunctionEx}
	 * @return a function
	 */
	static <T, R> Function<T, R> uncheckedOf(FunctionEx<T, R> checkedFn) {
		return t -> {
			try {
				return checkedFn.apply(t);
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}

	/**
	 * A function that returns passed value as is. Does not throw any exception
	 */
	static <T> FunctionEx<T, T> identity() {
		return t -> t;
	}
}
