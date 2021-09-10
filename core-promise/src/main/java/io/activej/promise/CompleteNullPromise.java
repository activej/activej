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

package io.activej.promise;

import org.jetbrains.annotations.Nullable;

/**
 * Represents a {@link CompletePromise} with result that equals {@code null}.
 * Optimized for multiple reuse since its result never changes.
 */
final class CompleteNullPromise<T> extends CompletePromise<T> {
	static final CompleteNullPromise<?> INSTANCE = new CompleteNullPromise<>();

	@SuppressWarnings("unchecked")
	static <T> CompleteNullPromise<T> instance() {
		return (CompleteNullPromise<T>) INSTANCE;
	}

	private CompleteNullPromise() {}

	@Override
	public @Nullable T getResult() {
		return null;
	}
}
