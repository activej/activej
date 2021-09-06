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

import java.util.function.Consumer;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * Represents a {@link Consumer} capable of throwing exceptions
 */
@FunctionalInterface
public interface ConsumerEx<T> {
	void accept(T t) throws Exception;

	/**
	 * Creates a {@code ConsumerEx} out of {@link Consumer}
	 * <p>
	 * If given consumer throws {@link UncheckedException}, its cause will be propagated
	 *
	 * @param uncheckedFn original {@link Consumer}
	 * @return a consumer capable of throwing exceptions
	 */
	static <T> ConsumerEx<T> of(Consumer<T> uncheckedFn) {
		return t -> {
			try {
				uncheckedFn.accept(t);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link Consumer} out of {@code ConsumerEx}
	 * <p>
	 * If given consumer throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code ConsumerEx}
	 * @return a consumer
	 */
	static <T> Consumer<T> uncheckedOf(ConsumerEx<T> checkedFn) {
		return t -> {
			try {
				checkedFn.accept(t);
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
