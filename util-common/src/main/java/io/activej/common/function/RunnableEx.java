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

import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * Represents a {@link Runnable} capable of throwing exceptions
 */
@FunctionalInterface
public interface RunnableEx {
	void run() throws Exception;

	/**
	 * Creates a {@code RunnableEx} out of {@link Runnable}
	 * <p>
	 * If given runnable throws {@link UncheckedException}, its cause will be propagated
	 *
	 * @param uncheckedFn original {@link Runnable}
	 * @return a runnable capable of throwing exceptions
	 */
	static RunnableEx of(Runnable uncheckedFn) {
		return () -> {
			try {
				uncheckedFn.run();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link Runnable} out of {@code RunnableEx}
	 * <p>
	 * If given runnable throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code RunnableEx}
	 * @return a runnable
	 */
	static Runnable uncheckedOf(RunnableEx checkedFn) {
		return () -> {
			try {
				checkedFn.run();
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
