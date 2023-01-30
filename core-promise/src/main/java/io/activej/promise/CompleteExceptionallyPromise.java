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
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.reactor.util.RunnableWithContext.wrapContext;

/**
 * Represents a {@code Promise} which is completed with an exception.
 */
@SuppressWarnings("unchecked")
public final class CompleteExceptionallyPromise<T> implements Promise<T> {
	private final Exception exception;

	public CompleteExceptionallyPromise(Exception e) {
		this.exception = e;
	}

	@Override
	public boolean isComplete() {
		return true;
	}

	@Override
	public boolean isResult() {
		return false;
	}

	@Override
	public boolean isException() {
		return true;
	}

	@Override
	public T getResult() {
		return null;
	}

	@Override
	public Exception getException() {
		return exception;
	}

	@Override
	public Try<T> getTry() {
		return Try.ofException(exception);
	}

	@Override
	public <U> Promise<U> next(NextPromise<T, U> promise) {
		promise.accept(null, exception);
		return promise;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn) {
		return (Promise<U>) this;
	}

	@Override
	public <U> Promise<U> mapIfElse(Predicate<? super T> predicate, FunctionEx<? super T, ? extends U> fn, FunctionEx<? super T, ? extends U> fnElse) {
		return (Promise<U>) this;
	}

	@Override
	public Promise<T> mapIf(Predicate<? super T> predicate, FunctionEx<? super T, ? extends T> fn) {
		return this;
	}

	@Override
	public Promise<T> mapIfNull(SupplierEx<? extends T> supplier) {
		return this;
	}

	@Override
	public <U> Promise<U> mapIfNonNull(FunctionEx<? super T, ? extends U> fn) {
		return (Promise<U>) this;
	}

	@Override
	public <U> Promise<U> map(BiFunctionEx<? super T, Exception, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(null, exception));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn, FunctionEx<Exception, ? extends U> exceptionFn) {
		try {
			return Promise.of(exceptionFn.apply(exception));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> mapException(FunctionEx<Exception, Exception> exceptionFn) {
		try {
			return Promise.ofException(exceptionFn.apply(exception));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> mapException(Predicate<Exception> predicate, FunctionEx<Exception, Exception> exceptionFn) {
		try {
			return predicate.test(exception) ? Promise.ofException(exceptionFn.apply(exception)) : this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <E extends Exception> Promise<T> mapException(Class<E> clazz, FunctionEx<E, Exception> exceptionFn) {
		try {
			return clazz.isAssignableFrom(exception.getClass()) ? Promise.ofException(exceptionFn.apply((E) exception)) : this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> then(FunctionEx<? super T, Promise<? extends U>> fn) {
		return (Promise<U>) this;
	}

	@Override
	public <U> Promise<U> thenIfElse(Predicate<? super T> predicate, FunctionEx<? super T, Promise<? extends U>> fn, FunctionEx<? super T, Promise<? extends U>> fnElse) {
		return (Promise<U>) this;
	}

	@Override
	public Promise<T> thenIf(Predicate<? super T> predicate, FunctionEx<? super T, Promise<? extends T>> fn) {
		return this;
	}

	@Override
	public Promise<T> thenIfNull(SupplierEx<Promise<? extends T>> supplier) {
		return this;
	}

	@Override
	public <U> Promise<U> thenIfNonNull(FunctionEx<? super T, Promise<? extends U>> fn) {
		return (Promise<U>) this;
	}

	@Override
	public <U> Promise<U> then(SupplierEx<Promise<? extends U>> fn) {
		return (Promise<U>) this;
	}

	@Override
	public <U> Promise<U> then(BiFunctionEx<? super T, Exception, Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(null, exception);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> then(
			FunctionEx<? super T, Promise<? extends U>> fn,
			FunctionEx<Exception, Promise<? extends U>> exceptionFn) {
		try {
			return (Promise<U>) exceptionFn.apply(exception);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> when(BiPredicate<? super T, @Nullable Exception> predicate, BiConsumerEx<? super T, Exception> fn) {
		try {
			if (predicate.test(null, exception)) {
				fn.accept(null, exception);
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> when(BiPredicate<? super T, @Nullable Exception> predicate, @Nullable ConsumerEx<? super T> fn, @Nullable ConsumerEx<Exception> exceptionFn) {
		try {
			if (predicate.test(null, exception)) {
				//noinspection ConstantConditions
				exceptionFn.accept(exception);
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> when(BiPredicate<? super T, @Nullable Exception> predicate, RunnableEx action) {
		try {
			if (predicate.test(null, exception)) {
				action.run();
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenComplete(BiConsumerEx<? super T, Exception> fn) {
		try {
			fn.accept(null, exception);
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenComplete(ConsumerEx<? super T> fn, ConsumerEx<Exception> exceptionFn) {
		try {
			exceptionFn.accept(exception);
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenComplete(RunnableEx action) {
		try {
			action.run();
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenResult(ConsumerEx<? super T> fn) {
		return this;
	}

	@Override
	public Promise<T> whenResult(Predicate<? super T> predicate, ConsumerEx<? super T> fn) {
		return this;
	}

	@Override
	public Promise<T> whenResult(RunnableEx action) {
		return this;
	}

	@Override
	public Promise<T> whenResult(Predicate<? super T> predicate, RunnableEx action) {
		return this;
	}

	@Override
	public Promise<T> whenException(ConsumerEx<Exception> fn) {
		try {
			fn.accept(exception);
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenException(Predicate<Exception> predicate, ConsumerEx<Exception> fn) {
		try {
			if (predicate.test(exception)) {
				fn.accept(exception);
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <E extends Exception> Promise<T> whenException(Class<E> clazz, ConsumerEx<E> fn) {
		try {
			if (clazz.isAssignableFrom(exception.getClass())) {
				fn.accept((E) exception);
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenException(RunnableEx action) {
		try {
			action.run();
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenException(Predicate<Exception> predicate, RunnableEx action) {
		try {
			if (predicate.test(exception)) {
				action.run();
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenException(Class<? extends Exception> clazz, RunnableEx action) {
		try {
			if (clazz.isAssignableFrom(exception.getClass())) {
				action.run();
			}
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U, V> Promise<V> combine(Promise<? extends U> other, BiFunctionEx<? super T, ? super U, ? extends V> fn) {
		other.whenResult(Recyclers::recycle);
		return (Promise<V>) this;
	}

	@Override
	public Promise<Void> both(Promise<?> other) {
		other.whenResult(Recyclers::recycle);
		return (Promise<Void>) this;
	}

	@Override
	public Promise<T> either(Promise<? extends T> other) {
		return (Promise<T>) other;
	}

	@Override
	public Promise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentReactor().post(wrapContext(result, () -> result.setException(exception)));
		return result;
	}

	@Override
	public Promise<Try<T>> toTry() {
		return Promise.of(Try.ofException(exception));
	}

	@Override
	public Promise<Void> toVoid() {
		return (Promise<Void>) this;
	}

	@Override
	public void run(Callback<? super T> cb) {
		cb.accept(null, exception);
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(exception);
		return future;
	}
}
