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

package io.activej.datastream.processor.transformer;

import io.activej.datastream.supplier.StreamSupplier;

/**
 * A transformer function that converts {@link StreamSupplier suppliers} into something else.
 * Part of the {@link StreamSupplier#transformWith} DSL.
 */
@FunctionalInterface
public interface StreamSupplierTransformer<T, R> {
	R transform(StreamSupplier<T> supplier);

	static <T> StreamSupplierTransformer<T, StreamSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
