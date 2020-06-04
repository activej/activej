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

import io.activej.async.callback.Callback;
import io.activej.common.collection.Try;
import io.activej.common.exception.UncheckedException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

/**
 * Represents a completed {@code Promise} with a result of unspecified type.
 */
@SuppressWarnings("unchecked")
public abstract class CompletePromise<T> implements Promise<T> {
	@Override
	public final boolean isComplete() {
		return true;
	}

	@Override
	public final boolean isResult() {
		return true;
	}

	@Override
	public final boolean isException() {
		return false;
	}

	@Override
	abstract public T getResult();

	@Override
	public final Throwable getException() {
		return null;
	}

	@Override
	public Try<T> getTry() {
		return Try.of(getResult());
	}

	@NotNull
	@Override
	public final <U, S extends Callback<? super T> & Promise<U>> Promise<U> next(@NotNull S promise) {
		promise.accept(getResult(), null);
		return promise;
	}

	@NotNull
	@Override
	public final <U> Promise<U> map(@NotNull Function<? super T, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult()));
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@NotNull
	@Override
	public final <U> Promise<U> mapEx(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult(), null));
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@SuppressWarnings("unchecked")
	@NotNull
	@Override
	public final <U> Promise<U> then(@NotNull Function<? super T, ? extends Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(getResult());
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@Override
	public @NotNull <U> Promise<U> then(@NotNull Supplier<? extends Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.get();
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@SuppressWarnings("unchecked")
	@NotNull
	@Override
	public final <U> Promise<U> thenEx(@NotNull BiFunction<? super T, Throwable, ? extends Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(getResult(), null);
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@NotNull
	@Override
	public final Promise<T> whenComplete(@NotNull Callback<? super T> action) {
		action.accept(getResult(), null);
		return this;
	}

	@Override
	public @NotNull Promise<T> whenComplete(@NotNull Runnable action) {
		action.run();
		return this;
	}

	@NotNull
	@Override
	public final Promise<T> whenResult(@NotNull Consumer<? super T> action) {
		action.accept(getResult());
		return this;
	}

	@Override
	public Promise<T> whenResult(@NotNull Runnable action) {
		action.run();
		return this;
	}

	@Override
	public final Promise<T> whenException(@NotNull Consumer<Throwable> action) {
		return this;
	}

	@Override
	public Promise<T> whenException(@NotNull Runnable action) {
		return this;
	}

	@NotNull
	@Override
	public final <U, V> Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
		if (other.isComplete()) {
			if (other.isResult()) {
				try {
					return Promise.of(fn.apply(getResult(), other.getResult()));
				} catch (UncheckedException u) {
					return Promise.ofException(u.getCause());
				}
			}
			return (Promise<V>) other;
		}
		return other.map(otherResult -> fn.apply(getResult(), otherResult));
	}

	@NotNull
	@Override
	public final Promise<Void> both(@NotNull Promise<?> other) {
		return other.toVoid();
	}

	@NotNull
	@Override
	public final Promise<T> either(@NotNull Promise<? extends T> other) {
		return this;
	}

	@NotNull
	@Override
	public final Promise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentEventloop().post(wrapContext(result, () -> result.set(getResult())));
		return result;
	}

	@NotNull
	@Override
	public final Promise<Try<T>> toTry() {
		return Promise.of(Try.of(getResult()));
	}

	@NotNull
	@Override
	public final Promise<Void> toVoid() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public final CompletableFuture<T> toCompletableFuture() {
		return CompletableFuture.completedFuture(getResult());
	}
}
