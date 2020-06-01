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

public final class ResultWithPromise<T, V> {
	private final T result;
	private final Promise<V> promise;

	private ResultWithPromise(T result, Promise<V> promise) {
		this.result = result;
		this.promise = promise;
	}

	public static <T, V> ResultWithPromise<T, V> of(T result, Promise<V> promise) {
		return new ResultWithPromise<>(result, promise);
	}

	public T getResult() {
		return result;
	}

	public Promise<V> getPromise() {
		return promise;
	}
}
