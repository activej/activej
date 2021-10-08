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
import io.activej.common.function.*;
import io.activej.common.recycle.Recyclers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

/**
 * Represents a completed {@code Promise} with a result of unspecified type.
 */
@SuppressWarnings("unchecked")
abstract class CompletePromise<T> implements Promise<T> {
	static {
		Recyclers.register(CompletePromise.class, promise -> Recyclers.recycle(promise.getResult()));
	}

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
	public final Exception getException() {
		return null;
	}

	@Override
	public Try<T> getTry() {
		return Try.of(getResult());
	}

	@Override
	public final <U> @NotNull Promise<U> next(@NotNull NextPromise<T, U> promise) {
		promise.accept(getResult(), null);
		return promise;
	}

	@Override
	public final <U> @NotNull Promise<U> map(@NotNull FunctionEx<? super T, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

    @Override
    public @NotNull <U> Promise<U> mapIfElse(@NotNull Predicate<? super T> predicate, @NotNull FunctionEx<? super T, ? extends U> fn, @NotNull FunctionEx<? super T, ? extends U> fnElse) {
		try {
			return Promise.of(predicate.test(getResult()) ? fn.apply(getResult()) : fnElse.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> mapIf(@NotNull Predicate<? super T> predicate, @NotNull FunctionEx<? super T, ? extends T> fn) {
		try {
			T result = getResult();
			return Promise.of(predicate.test(result) ? fn.apply(result) : result);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> mapIfNull(@NotNull SupplierEx<? extends T> supplier) {
		try {
			T result = getResult();
			return Promise.of(result == null ? supplier.get() : result);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull <U> Promise<U> mapIfNonNull(@NotNull FunctionEx<? super @NotNull T, ? extends U> fn) {
		try {
			T result = getResult();
			return Promise.of(result != null ? fn.apply(result) : null);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public final <U> @NotNull Promise<U> map(@NotNull BiFunctionEx<? super T, Exception, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult(), null));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> @NotNull Promise<U> map(@NotNull FunctionEx<? super T, ? extends U> fn, @NotNull FunctionEx<@NotNull Exception, ? extends U> exceptionFn) {
		try {
			return Promise.of(fn.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> mapException(@NotNull FunctionEx<@NotNull Exception, Exception> exceptionFn) {
		return this;
	}

	@Override
	public @NotNull Promise<T> mapException(@NotNull Predicate<Exception> predicate, @NotNull FunctionEx<@NotNull Exception, @NotNull Exception> exceptionFn) {
		return this;
	}

	@Override
	public @NotNull Promise<T> mapException(@NotNull Class<? extends Exception> clazz, @NotNull FunctionEx<@NotNull Exception, @NotNull Exception> exceptionFn) {
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <U> @NotNull Promise<U> then(@NotNull FunctionEx<? super T, Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(getResult());
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull <U> Promise<U> thenIfElse(@NotNull Predicate<? super T> predicate, @NotNull FunctionEx<? super T, Promise<? extends U>> fn, @NotNull FunctionEx<? super T, Promise<? extends U>> fnElse) {
		try {
			return (Promise<U>) (predicate.test(getResult()) ? fn.apply(getResult()) : fnElse.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> thenIf(@NotNull Predicate<? super T> predicate, @NotNull FunctionEx<? super T, Promise<? extends T>> fn) {
		try {
			return (Promise<T>) (predicate.test(getResult()) ? fn.apply(getResult()) : this);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> thenIfNull(@NotNull SupplierEx<Promise<? extends T>> supplier) {
		try {
			return (Promise<T>) (getResult() == null ? supplier.get() : this);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull <U> Promise<U> thenIfNonNull(@NotNull FunctionEx<? super @NotNull T, Promise<? extends U>> fn) {
		try {
			T result = getResult();
			return (Promise<U>) (result != null ? fn.apply(result) : this);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> @NotNull Promise<U> then(@NotNull SupplierEx<Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.get();
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <U> @NotNull Promise<U> then(@NotNull BiFunctionEx<? super T, Exception, Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(getResult(), null);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> @NotNull Promise<U> then(
			@NotNull FunctionEx<? super T, Promise<? extends U>> fn,
			@NotNull FunctionEx<@NotNull Exception, Promise<? extends U>> exceptionFn) {
		try {
			return (Promise<U>) fn.apply(getResult());
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> when(@NotNull BiPredicate<? super T, @Nullable Exception> predicate, @NotNull BiConsumerEx<? super T, Exception> fn) {
		try {
			if (predicate.test(getResult(), null)) {
				fn.accept(getResult(), null);
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> when(@NotNull BiPredicate<? super T, @Nullable Exception> predicate, @Nullable ConsumerEx<? super T> fn, @Nullable ConsumerEx<@NotNull Exception> exceptionFn) {
		try {
			if (predicate.test(getResult(), null)) {
				//noinspection ConstantConditions
				fn.accept(getResult());
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> when(@NotNull BiPredicate<? super T, @Nullable Exception> predicate, @NotNull RunnableEx action) {
		try {
			if (predicate.test(getResult(), null)) {
				action.run();
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenComplete(@NotNull BiConsumerEx<? super T, Exception> fn) {
		try {
			fn.accept(getResult(), null);
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenComplete(@NotNull ConsumerEx<? super T> fn, @NotNull ConsumerEx<@NotNull Exception> exceptionFn) {
		try {
			fn.accept(getResult());
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenComplete(@NotNull RunnableEx action) {
		try {
			action.run();
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenResult(ConsumerEx<? super T> fn) {
		try {
			fn.accept(getResult());
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenResult(@NotNull Predicate<? super T> predicate, ConsumerEx<? super T> fn) {
		try {
			if (predicate.test(getResult())) {
				fn.accept(getResult());
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenResult(@NotNull RunnableEx action) {
		try {
			action.run();
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenResult(@NotNull Predicate<? super T> predicate, @NotNull RunnableEx action) {
		try {
			if (predicate.test(getResult())) {
				action.run();
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public @NotNull Promise<T> whenException(@NotNull ConsumerEx<Exception> fn) {
		return this;
	}

	@Override
	public @NotNull Promise<T> whenException(@NotNull Predicate<Exception> predicate, @NotNull ConsumerEx<@NotNull Exception> fn) {
		return this;
	}

	@Override
	public @NotNull Promise<T> whenException(@NotNull Class<? extends Exception> clazz, @NotNull ConsumerEx<@NotNull Exception> fn) {
		return this;
	}

	@Override
	public @NotNull Promise<T> whenException(@NotNull RunnableEx action) {
		return this;
	}

	@Override
	public @NotNull Promise<T> whenException(@NotNull Predicate<Exception> predicate, @NotNull RunnableEx action) {
		return this;
	}

	@Override
	public @NotNull Promise<T> whenException(@NotNull Class<? extends Exception> clazz, @NotNull RunnableEx action) {
		return this;
	}

	@Override
	public final <U, V> @NotNull Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunctionEx<? super T, ? super U, ? extends V> fn) {
		return (Promise<V>) other
				.map(otherResult -> fn.apply(this.getResult(), otherResult))
				.whenException(() -> Recyclers.recycle(this.getResult()));
	}

	@Override
	public final @NotNull Promise<Void> both(@NotNull Promise<?> other) {
		Recyclers.recycle(getResult());
		return other.map(AbstractPromise::recycleToVoid);
	}

	@Override
	public final @NotNull Promise<T> either(@NotNull Promise<? extends T> other) {
		other.whenResult(Recyclers::recycle);
		return this;
	}

	@Override
	public final @NotNull Promise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentEventloop().post(wrapContext(result, () -> result.set(getResult())));
		return result;
	}

	@Override
	public final @NotNull Promise<Try<T>> toTry() {
		return Promise.of(Try.of(getResult()));
	}

	@Override
	public final @NotNull Promise<Void> toVoid() {
		return Promise.complete();
	}

	@Override
	public void run(@NotNull Callback<? super T> callback) {
		callback.accept(getResult(), null);
	}

	@Override
	public final @NotNull CompletableFuture<T> toCompletableFuture() {
		return CompletableFuture.completedFuture(getResult());
	}
}
