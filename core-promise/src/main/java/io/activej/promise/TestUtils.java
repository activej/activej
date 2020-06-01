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

import io.activej.eventloop.Eventloop;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestUtils {
	public static <T> T await(Promise<T> promise) {
		try {
			return compute(promise);
		} catch (ExecutionException e) {
			throw new AssertionError(e.getCause());
		}
	}

	@SafeVarargs
	public static <T> Void await(Promise<T>... promises) {
		return await(Promises.all(promises));
	}

	@SuppressWarnings("unchecked")
	public static <T, E extends Throwable> E awaitException(Promise<T> promise) {
		try {
			compute(promise);
		} catch (ExecutionException e) {
			return (E) e.getCause();
		} catch (Throwable e) {
			return (E) e;
		}
		throw new AssertionError("Promise did not fail");
	}

	@SafeVarargs
	public static <T, E extends Throwable> E awaitException(Promise<T>... promises) {
		return awaitException(Promises.all(promises));
	}

	private static <T> T compute(Promise<T> promise) throws ExecutionException {
		Future<T> future = promise.toCompletableFuture();
		Eventloop.getCurrentEventloop().run();
		try {
			return future.get();
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		}
	}
}
