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

import java.util.function.Supplier;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * Represents a {@link Supplier} capable of throwing exceptions
 */
@FunctionalInterface
public interface SupplierEx<T> {
	T get() throws Exception;

	/**
	 * Creates a {@code SupplierEx} out of {@link Supplier}
	 * <p>
	 * If given supplier throws {@link UncheckedException}, its cause will be propagated
	 *
	 * @param uncheckedFn original {@link Supplier}
	 * @return a supplier capable of throwing exceptions
	 */
	static <T> SupplierEx<T> of(Supplier<T> uncheckedFn) {
		return () -> {
			try {
				return uncheckedFn.get();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	/**
	 * Creates a {@link Supplier} out of {@code SupplierEx}
	 * <p>
	 * If given supplier throws a checked exception, it will be wrapped into {@link UncheckedException}
	 * and rethrown
	 * <p>
	 * Unchecked exceptions will be handled by thread's {@link FatalErrorHandler}
	 *
	 * @param checkedFn original {@code SupplierEx}
	 * @return a supplier
	 */
	static <T> Supplier<T> uncheckedOf(SupplierEx<T> checkedFn) {
		return () -> {
			try {
				return checkedFn.get();
			} catch (Exception ex) {
				handleError(ex, checkedFn);
				throw UncheckedException.of(ex);
			}
		};
	}
}
