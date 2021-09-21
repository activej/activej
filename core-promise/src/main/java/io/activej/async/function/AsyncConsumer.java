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
import io.activej.common.function.ConsumerEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * Represents an asynchronous consumer that consumes data items.
 */
@FunctionalInterface
public interface AsyncConsumer<T> {
	/**
	 * Consumes some data asynchronously.
	 *
	 * @param value value to be consumed
	 * @return {@link Promise} of {@link Void} that represents successful consumption of data
	 */
	@NotNull Promise<Void> accept(T value);

	/**
	 * Wraps a {@link ConsumerEx} interface.
	 *
	 * @param consumer a {@link ConsumerEx}
	 * @return {@link AsyncConsumer} that works on top of {@link ConsumerEx} interface
	 */
	static @NotNull <T> AsyncConsumer<T> of(@NotNull ConsumerEx<? super T> consumer) {
		return value -> {
			try {
				consumer.accept(value);
			} catch (Exception e) {
				handleError(e, consumer);
				return Promise.ofException(e);
			}
			return Promise.complete();
		};
	}

	static <T> AsyncConsumer<T> cast(AsyncConsumer<T> consumer){
		return consumer;
	}

	@Contract(pure = true)
	default @NotNull <R> R transformWith(@NotNull Function<AsyncConsumer<T>, R> fn) {
		return fn.apply(this);
	}

	@Contract(pure = true)
	default @NotNull AsyncConsumer<T> async() {
		return value -> accept(value).async();
	}

	@Contract(pure = true)
	default @NotNull AsyncConsumer<T> withExecutor(@NotNull AsyncExecutor asyncExecutor) {
		return value -> asyncExecutor.execute(() -> accept(value));
	}
}
