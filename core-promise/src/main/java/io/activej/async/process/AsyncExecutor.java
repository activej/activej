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

package io.activej.async.process;

import io.activej.async.function.AsyncSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

public interface AsyncExecutor {
	@NotNull <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) throws RejectedExecutionException;

	default @NotNull Promise<Void> run(@NotNull Runnable runnable) throws RejectedExecutionException {
		return execute(() -> {
			runnable.run();
			return Promise.complete();
		});
	}

	default @NotNull <T> Promise<T> call(@NotNull Callable<T> callable) throws RejectedExecutionException {
		return execute(() -> {
			T result;
			try {
				result = callable.call();
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				return Promise.ofException(e);
			}
			return Promise.of(result);
		});
	}
}
