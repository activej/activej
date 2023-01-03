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

import io.activej.common.function.SupplierEx;
import io.activej.promise.Promise;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * Represents asynchronous supplier that returns {@link Promise} of some data.
 */
@FunctionalInterface
public interface AsyncSupplier<T> {
	/**
	 * Gets {@link Promise} of data item asynchronously.
	 */
	Promise<T> get();

	/**
	 * Wraps a {@link SupplierEx} interface.
	 *
	 * @param supplier a {@link SupplierEx}
	 * @return {@link AsyncSupplier} that works on top of {@link SupplierEx} interface
	 */
	static <T> AsyncSupplier<T> of(SupplierEx<T> supplier) {
		return () -> {
			try {
				return Promise.of(supplier.get());
			} catch (Exception e) {
				handleError(e, supplier);
				return Promise.ofException(e);
			}
		};
	}
}
