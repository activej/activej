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

import java.util.function.BiConsumer;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * Represents a {@link BiConsumer} capable of throwing exceptions
 */
@FunctionalInterface
public interface BiConsumerEx<T, U> {
	void accept(T t, U u) throws Exception;

	/**
	 * Creates a {@code BiConsumerEx} out of {@link BiConsumer}
	 * <p>
	 * If given consumer throws {@link UncheckedException}, its cause will be propagated
	 *
	 * @param uncheckedFn original {@link BiConsumer}
	 * @return a consumer capable of throwing exceptions
	 */
	static <T, U> BiConsumerEx<T, U> of(BiConsumer<T, U> uncheckedFn) {
		return (t, u) -> {
			try {
				uncheckedFn.accept(t, u);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link BiConsumer} out of {@code BiConsumerEx}
	 * <p>
	 * If given consumer throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code BiConsumerEx}
	 * @return a consumer
	 */
	static <T, U> BiConsumer<T, U> uncheckedOf(BiConsumerEx<T, U> checkedFn) {
		return (t, u) -> {
			try {
				checkedFn.accept(t, u);
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
