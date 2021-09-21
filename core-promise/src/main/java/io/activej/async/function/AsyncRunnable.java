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
import io.activej.common.function.RunnableEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@FunctionalInterface
public interface AsyncRunnable {
	Promise<Void> run();

	/**
	 * Wraps a {@link RunnableEx} interface.
	 *
	 * @param runnable a {@link RunnableEx}
	 * @return {@link AsyncRunnable} that works on top of {@link RunnableEx} interface
	 */
	static @NotNull AsyncRunnable of(@NotNull RunnableEx runnable) {
		return () -> {
			try {
				runnable.run();
			} catch (Exception e) {
				handleError(e, runnable);
				return Promise.ofException(e);
			}
			return Promise.complete();
		};
	}

	static AsyncRunnable cast(AsyncRunnable function) {
		return function;
	}

	@Contract(pure = true)
	default <R> @NotNull R transformWith(@NotNull Function<AsyncRunnable, R> fn) {
		return fn.apply(this);
	}

	@Contract(pure = true)
	default @NotNull AsyncRunnable async() {
		return () -> run().async();
	}

	@Contract(pure = true)
	default @NotNull AsyncRunnable withExecutor(@NotNull AsyncExecutor asyncExecutor) {
		return () -> asyncExecutor.execute(this::run);
	}

}
