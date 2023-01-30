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
 * Represents a completed {@code Promise} with a result of unspecified type.
 */
@SuppressWarnings("unchecked")
public abstract class CompletePromise<T> implements Promise<T> {
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
	public final <U> Promise<U> next(NextPromise<T, U> promise) {
		promise.accept(getResult(), null);
		return promise;
	}

	@Override
	public final <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> mapIfElse(Predicate<? super T> predicate, FunctionEx<? super T, ? extends U> fn, FunctionEx<? super T, ? extends U> fnElse) {
		try {
			return Promise.of(predicate.test(getResult()) ? fn.apply(getResult()) : fnElse.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> mapIf(Predicate<? super T> predicate, FunctionEx<? super T, ? extends T> fn) {
		try {
			T result = getResult();
			return Promise.of(predicate.test(result) ? fn.apply(result) : result);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> mapIfNull(SupplierEx<? extends T> supplier) {
		try {
			T result = getResult();
			return Promise.of(result == null ? supplier.get() : result);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> mapIfNonNull(FunctionEx<? super T, ? extends U> fn) {
		try {
			T result = getResult();
			return Promise.of(result != null ? fn.apply(result) : null);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public final <U> Promise<U> map(BiFunctionEx<? super T, Exception, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult(), null));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn, FunctionEx<Exception, ? extends U> exceptionFn) {
		try {
			return Promise.of(fn.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> mapException(FunctionEx<Exception, Exception> exceptionFn) {
		return this;
	}

	@Override
	public Promise<T> mapException(Predicate<Exception> predicate, FunctionEx<Exception, Exception> exceptionFn) {
		return this;
	}

	@Override
	public <E extends Exception> Promise<T> mapException(Class<E> clazz, FunctionEx<E, Exception> exceptionFn) {
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <U> Promise<U> then(FunctionEx<? super T, Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(getResult());
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> thenIfElse(Predicate<? super T> predicate, FunctionEx<? super T, Promise<? extends U>> fn, FunctionEx<? super T, Promise<? extends U>> fnElse) {
		try {
			return (Promise<U>) (predicate.test(getResult()) ? fn.apply(getResult()) : fnElse.apply(getResult()));
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> thenIf(Predicate<? super T> predicate, FunctionEx<? super T, Promise<? extends T>> fn) {
		try {
			return (Promise<T>) (predicate.test(getResult()) ? fn.apply(getResult()) : this);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> thenIfNull(SupplierEx<Promise<? extends T>> supplier) {
		try {
			return (Promise<T>) (getResult() == null ? supplier.get() : this);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> thenIfNonNull(FunctionEx<? super T, Promise<? extends U>> fn) {
		try {
			T result = getResult();
			return (Promise<U>) (result != null ? fn.apply(result) : this);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> then(SupplierEx<Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.get();
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <U> Promise<U> then(BiFunctionEx<? super T, Exception, Promise<? extends U>> fn) {
		try {
			return (Promise<U>) fn.apply(getResult(), null);
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
			return (Promise<U>) fn.apply(getResult());
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> when(BiPredicate<? super T, @Nullable Exception> predicate, BiConsumerEx<? super T, Exception> fn) {
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
	public Promise<T> when(BiPredicate<? super T, @Nullable Exception> predicate, @Nullable ConsumerEx<? super T> fn, @Nullable ConsumerEx<Exception> exceptionFn) {
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
	public Promise<T> when(BiPredicate<? super T, @Nullable Exception> predicate, RunnableEx action) {
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
	public Promise<T> whenComplete(BiConsumerEx<? super T, Exception> fn) {
		try {
			fn.accept(getResult(), null);
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenComplete(ConsumerEx<? super T> fn, ConsumerEx<Exception> exceptionFn) {
		try {
			fn.accept(getResult());
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
		try {
			fn.accept(getResult());
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenResult(Predicate<? super T> predicate, ConsumerEx<? super T> fn) {
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
	public Promise<T> whenResult(RunnableEx action) {
		try {
			action.run();
			return this;
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public Promise<T> whenResult(Predicate<? super T> predicate, RunnableEx action) {
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
	public Promise<T> whenException(ConsumerEx<Exception> fn) {
		return this;
	}

	@Override
	public Promise<T> whenException(Predicate<Exception> predicate, ConsumerEx<Exception> fn) {
		return this;
	}

	@Override
	public <E extends Exception> Promise<T> whenException(Class<E> clazz, ConsumerEx<E> fn) {
		return this;
	}

	@Override
	public Promise<T> whenException(RunnableEx action) {
		return this;
	}

	@Override
	public Promise<T> whenException(Predicate<Exception> predicate, RunnableEx action) {
		return this;
	}

	@Override
	public Promise<T> whenException(Class<? extends Exception> clazz, RunnableEx action) {
		return this;
	}

	@Override
	public final <U, V> Promise<V> combine(Promise<? extends U> other, BiFunctionEx<? super T, ? super U, ? extends V> fn) {
		return (Promise<V>) other
				.map(otherResult -> fn.apply(this.getResult(), otherResult))
				.whenException(() -> Recyclers.recycle(this.getResult()));
	}

	@Override
	public final Promise<Void> both(Promise<?> other) {
		Recyclers.recycle(getResult());
		return other.map(AbstractPromise::recycleToVoid);
	}

	@Override
	public final Promise<T> either(Promise<? extends T> other) {
		other.whenResult(Recyclers::recycle);
		return this;
	}

	@Override
	public final Promise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentReactor().post(wrapContext(result, () -> result.set(getResult())));
		return result;
	}

	@Override
	public final Promise<Try<T>> toTry() {
		return Promise.of(Try.of(getResult()));
	}

	@Override
	public final Promise<Void> toVoid() {
		return Promise.complete();
	}

	@Override
	public void run(Callback<? super T> cb) {
		cb.accept(getResult(), null);
	}

	@Override
	public final CompletableFuture<T> toCompletableFuture() {
		return CompletableFuture.completedFuture(getResult());
	}
}
