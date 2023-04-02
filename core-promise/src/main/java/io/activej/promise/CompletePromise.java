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
import io.activej.async.function.*;
import io.activej.common.collection.Try;
import io.activej.common.function.*;
import io.activej.common.recycle.Recyclers;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

import static io.activej.common.exception.FatalErrorHandler.handleError;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.reactor.util.RunnableWithContext.runnableOf;

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
	public final <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult()));
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
	public <E extends Exception> Promise<T> mapException(Class<E> clazz,
			FunctionEx<? super E, ? extends Exception> exceptionFn) {
		return this;
	}

	@Override
	public final <U> Promise<U> then(AsyncFunctionEx<? super T, U> fn) {
		try {
			return fn.apply(getResult());
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackFunctionEx<? super T, U> fn) {
		try {
			return Promise.ofCallback(getResult(), fn);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> then(AsyncSupplierEx<U> fn) {
		try {
			return fn.get();
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackSupplierEx<U> fn) {
		try {
			return Promise.ofCallback(fn);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public final <U> Promise<U> then(AsyncBiFunctionEx<? super T, Exception, U> fn) {
		try {
			return fn.apply(getResult(), null);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackBiFunctionEx<? super T, @Nullable Exception, U> fn) {
		try {
			return Promise.ofCallback(getResult(), null, fn);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> then(
			AsyncFunctionEx<? super T, U> fn,
			AsyncFunctionEx<Exception, U> exceptionFn) {
		try {
			return fn.apply(getResult());
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackFunctionEx<? super T, U> fn, CallbackFunctionEx<Exception, U> exceptionFn) {
		try {
			return Promise.ofCallback(getResult(), null, fn, exceptionFn);
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
	public Promise<T> whenException(ConsumerEx<Exception> fn) {
		return this;
	}

	@Override
	public <E extends Exception> Promise<T> whenException(Class<E> clazz, ConsumerEx<? super E> fn) {
		return this;
	}

	@Override
	public Promise<T> whenException(RunnableEx action) {
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
		getCurrentReactor().post(runnableOf(result, () -> result.set(getResult())));
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
	public void next(NextPromise<? super T, ?> cb) {
		cb.acceptNext(getResult(), null);
	}

	@Override
	public Promise<T> subscribe(Callback<? super T> cb) {
		cb.accept(getResult(), null);
		return this;
	}

	@Override
	public final CompletableFuture<T> toCompletableFuture() {
		return CompletableFuture.completedFuture(getResult());
	}
}
