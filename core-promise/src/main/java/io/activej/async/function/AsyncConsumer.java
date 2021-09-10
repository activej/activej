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
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;

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
	 * Wraps standard Java's {@link Consumer} interface.
	 *
	 * @param consumer - Java's {@link Consumer} of Promises
	 * @return {@link AsyncConsumer} that works on top of standard Java's {@link Consumer} interface
	 */
	static @NotNull <T> AsyncConsumer<T> of(@NotNull Consumer<? super T> consumer) {
		return value -> {
			consumer.accept(value);
			return Promise.complete();
		};
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

	@Contract(pure = true)
	default @NotNull AsyncConsumer<T> peek(@NotNull Consumer<T> action) {
		return value -> {
			action.accept(value);
			return accept(value);
		};
	}

	@Contract(pure = true)
	default @NotNull <V> AsyncConsumer<V> map(@NotNull Function<? super V, ? extends T> fn) {
		return value -> accept(fn.apply(value));
	}

	@Contract(pure = true)
	default @NotNull <V> AsyncConsumer<V> mapAsync(@NotNull Function<? super V, ? extends Promise<T>> fn) {
		return value -> fn.apply(value).then(this::accept);
	}
}
