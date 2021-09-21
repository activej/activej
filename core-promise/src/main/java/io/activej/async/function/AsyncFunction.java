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

import io.activej.async.process.AsyncExecutor;
import io.activej.common.function.FunctionEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@FunctionalInterface
public interface AsyncFunction<T, R> {
	Promise<R> apply(T t);

	/**
	 * Wraps a {@link FunctionEx} interface.
	 *
	 * @param function a {@link FunctionEx}
	 * @return {@link AsyncFunction} that works on top of {@link FunctionEx} interface
	 */
	static @NotNull <T, R> AsyncFunction<T, R> of(@NotNull FunctionEx<? super T, ? extends R> function) {
		return value -> {
			try {
				return Promise.of(function.apply(value));
			} catch (Exception e) {
				handleError(e, function);
				return Promise.ofException(e);
			}
		};
	}

	static <T, R> AsyncFunction<T, R> cast(AsyncFunction<T, R> function){
		return function;
	}

	@Contract(pure = true)
	default @NotNull <V> V transformWith(@NotNull Function<AsyncFunction<T, R>, V> fn) {
		return fn.apply(this);
	}

	@Contract(pure = true)
	default @NotNull AsyncFunction<T, R> async() {
		return value -> apply(value).async();
	}

	@Contract(pure = true)
	default @NotNull AsyncFunction<T, R> withExecutor(@NotNull AsyncExecutor asyncExecutor) {
		return value -> asyncExecutor.execute(() -> apply(value));
	}
}
