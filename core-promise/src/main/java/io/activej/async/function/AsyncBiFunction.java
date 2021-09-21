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
import io.activej.common.function.BiFunctionEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@FunctionalInterface
public interface AsyncBiFunction<T, U, R> {
	Promise<R> apply(T t, U u);

	/**
	 * Wraps a {@link BiFunctionEx} interface.
	 *
	 * @param function a {@link BiFunctionEx}
	 * @return {@link AsyncBiFunction} that works on top of {@link BiFunctionEx} interface
	 */
	static @NotNull <T, U, R> AsyncBiFunction<T, U, R> of(@NotNull BiFunctionEx<? super T, ? super U, ? extends R> function) {
		return (t, u) -> {
			try {
				return Promise.of(function.apply(t, u));
			} catch (Exception e) {
				handleError(e, function);
				return Promise.ofException(e);
			}
		};
	}

	static <T, U, R> AsyncBiFunction<T, U, R> cast(AsyncBiFunction<T, U, R> function){
		return function;
	}

	@Contract(pure = true)
	default @NotNull <V> V transformWith(@NotNull Function<AsyncBiFunction<T, U, R>, V> fn) {
		return fn.apply(this);
	}

	@Contract(pure = true)
	default @NotNull AsyncBiFunction<T, U, R> async() {
		return (t, u) -> apply(t, u).async();
	}

	@Contract(pure = true)
	default @NotNull AsyncBiFunction<T, U, R> withExecutor(@NotNull AsyncExecutor asyncExecutor) {
		return (t, u) -> asyncExecutor.execute(() -> apply(t, u));
	}
}
