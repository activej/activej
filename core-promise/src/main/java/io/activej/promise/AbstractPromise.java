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
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.collection.Try;
import io.activej.common.function.*;
import io.activej.common.recycle.Recyclers;
import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import static io.activej.common.Checks.checkState;
import static io.activej.common.exception.FatalErrorHandler.handleError;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.reactor.util.RunnableWithContext.runnableOf;

@SuppressWarnings({"unchecked", "WeakerAccess", "unused"})
public abstract class AbstractPromise<T> implements Promise<T> {
	static {
		Recyclers.register(AbstractPromise.class, promise -> promise.whenResult(Recyclers::recycle));
	}

	private static final boolean CHECKS = Checks.isEnabled(AbstractPromise.class);

	private static final Object PROMISE_NOT_SET = new Object();
	private static final boolean RESET_CALLBACKS = ApplicationSettings.getBoolean(AbstractPromise.class, "resetCallbacks", false);

	protected T result = (T) PROMISE_NOT_SET;

	protected @Nullable Exception exception;

	// instanceof NextPromise<? super T, ?>
	// instanceof Callback<? super T>
	// instanceof Object[]
	protected @Nullable Object next;

	public void reset() {
		this.result = (T) PROMISE_NOT_SET;
		this.exception = null;
		this.next = null;
	}

	public void resetCallbacks() {
		this.next = null;
	}

	@Override
	public final boolean isComplete() {
		return result != PROMISE_NOT_SET;
	}

	@Override
	public final boolean isResult() {
		return result != PROMISE_NOT_SET && exception == null;
	}

	@Override
	public final boolean isException() {
		return exception != null;
	}

	@Override
	public T getResult() {
		return result != PROMISE_NOT_SET ? result : null;
	}

	@Override
	public Exception getException() {
		return exception;
	}

	@Override
	public Try<T> getTry() {
		if (isResult()) return Try.of(result);
		if (isException()) return Try.ofException(exception);
		return null;
	}

	protected void complete(@Nullable T value, @Nullable Exception e) {
		if (CHECKS) checkState(!isComplete(), "Promise has already been completed");
		if (e == null) {
			complete(value);
		} else {
			completeExceptionally(e);
		}
	}

	@SuppressWarnings({"rawtypes", "ForLoopReplaceableByForEach"})
	@Async.Execute
	protected void complete(@Nullable T value) {
		if (CHECKS) checkState(!isComplete(), "Promise has already been completed");
		result = value;
		if (next instanceof NextPromise cb) {
			cb.acceptNext(value, null);
		} else if (next instanceof Callback cb) {
			cb.accept(value, null);
		} else if (next instanceof Object[] list) {
			for (int i = 0; i < list.length; i++) {
				Object it = list[i];
				if (it instanceof NextPromise cb) {
					cb.acceptNext(value, null);
				} else {
					((Callback<T>) it).accept(value, null);
				}
			}
		}
		if (RESET_CALLBACKS) {
			next = null;
		}
	}

	@SuppressWarnings({"rawtypes", "ForLoopReplaceableByForEach"})
	@Async.Execute
	protected void completeExceptionally(@Nullable Exception e) {
		if (CHECKS) checkState(!isComplete(), "Promise has already been completed");
		result = null;
		exception = e;
		if (next instanceof NextPromise cb) {
			cb.acceptNext(null, exception);
		} else if (next instanceof Callback cb) {
			cb.accept(null, exception);
		} else if (next instanceof Object[] list) {
			for (int i = 0; i < list.length; i++) {
				Object it = list[i];
				if (it instanceof NextPromise cb) {
					cb.acceptNext(null, exception);
				} else {
					((Callback<?>) it).accept(null, exception);
				}
			}
		}
		if (RESET_CALLBACKS) {
			next = null;
		}
	}

	protected boolean tryComplete(@Nullable T value, @Nullable Exception e) {
		if (!isComplete()) {
			complete(value, e);
			return true;
		}
		return false;
	}

	protected boolean tryComplete(@Nullable T value) {
		if (!isComplete()) {
			complete(value);
			return true;
		}
		return false;
	}

	protected boolean tryCompleteExceptionally(Exception e) {
		if (!isComplete()) {
			completeExceptionally(e);
			return true;
		}
		return false;
	}

	@Override
	public <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.of(fn.apply(result)) : (Promise<U>) this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					U newResult;
					try {
						newResult = fn.apply(result);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					complete(newResult);
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".map(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> map(BiFunctionEx<? super T, Exception, ? extends U> fn) {
		if (isComplete()) {
			try {
				return Promise.of(fn.apply(result, exception));
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, Exception e) {
				U newResult;
				try {
					newResult = fn.apply(result, e);
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(newResult);
			}

			@Override
			public String describe() {
				return ".map(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn, FunctionEx<Exception, ? extends U> exceptionFn) {
		if (isComplete()) {
			try {
				return Promise.of(exception == null ? fn.apply(result) : exceptionFn.apply(exception));
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, Exception e) {
				U newResult;
				try {
					newResult = e == null ? fn.apply(result) : exceptionFn.apply(e);
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(newResult);
			}

			@Override
			public String describe() {
				return ".map(" + formatToString(fn) + ", " + formatToString(exceptionFn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> mapException(FunctionEx<Exception, Exception> exceptionFn) {
		if (isComplete()) {
			try {
				return exception == null ? this : Promise.ofException(exceptionFn.apply(exception));
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					complete(result);
				} else {
					try {
						e = exceptionFn.apply(e);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".mapException(" + formatToString(exceptionFn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <E extends Exception> Promise<T> mapException(Class<E> clazz,
			FunctionEx<? super E, ? extends Exception> exceptionFn) {
		if (isComplete()) {
			try {
				return exception == null ?
						this :
						clazz.isAssignableFrom(exception.getClass()) ? Promise.ofException(exceptionFn.apply((E) exception)) : this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					complete(result);
				} else {
					try {
						e = clazz.isAssignableFrom(e.getClass()) ? exceptionFn.apply((E) e) : e;
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".mapException(" + clazz.getName() + ", " + formatToString(exceptionFn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> then(AsyncSupplierEx<U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? fn.get() : (Promise<U>) this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					Promise<? extends U> promise;
					try {
						promise = fn.get();
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					promise.subscribe(this);
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackSupplierEx<U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.ofCallback(fn) : (Promise<U>) this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					try {
						fn.get(this);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
					}
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> then(AsyncFunctionEx<? super T, U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? fn.apply(result) : (Promise<U>) this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					Promise<? extends U> promise;
					try {
						promise = fn.apply(result);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					promise.subscribe(this);
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackFunctionEx<? super T, U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.ofCallback(result, fn) : (Promise<U>) this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					try {
						fn.apply(result, this);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
					}
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> then(AsyncBiFunctionEx<? super T, Exception, U> fn) {
		if (isComplete()) {
			try {
				return fn.apply(result, exception);
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					Promise<? extends U> promise;
					try {
						promise = fn.apply(result, null);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					promise.subscribe(this);
				} else {
					Promise<? extends U> promise;
					try {
						promise = fn.apply(null, e);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					promise.subscribe(this);
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackBiFunctionEx<? super T, @Nullable Exception, U> fn) {
		if (isComplete()) {
			try {
				return Promise.ofCallback(result, exception, fn);
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					try {
						fn.apply(result, null, this);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
					}
				} else {
					Promise<? extends U> promise;
					try {
						fn.apply(null, e, this);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
					}
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> then(AsyncFunctionEx<? super T, U> fn, AsyncFunctionEx<Exception, U> exceptionFn) {
		if (isComplete()) {
			try {
				return exception == null ? fn.apply(result) : exceptionFn.apply(exception);
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					Promise<? extends U> promise;
					try {
						promise = fn.apply(result);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					promise.subscribe(this);
				} else {
					Promise<? extends U> promise;
					try {
						promise = exceptionFn.apply(e);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					promise.subscribe(this);
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ", " + formatToString(exceptionFn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <U> Promise<U> thenCallback(CallbackFunctionEx<? super T, U> fn, CallbackFunctionEx<Exception, U> exceptionFn) {
		if (isComplete()) {
			try {
				return Promise.ofCallback(result, exception, fn, exceptionFn);
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					try {
						fn.apply(result, this);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
					}
				} else {
					try {
						exceptionFn.apply(e, this);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
					}
				}
			}

			@Override
			public String describe() {
				return ".then(" + formatToString(fn) + ", " + formatToString(exceptionFn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenComplete(BiConsumerEx<? super T, Exception> fn) {
		if (isComplete()) {
			try {
				fn.accept(result, exception);
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				try {
					fn.accept(result, e);
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(result, e);
			}

			@Override
			public String describe() {
				return ".whenComplete(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenComplete(ConsumerEx<? super T> fn, ConsumerEx<Exception> exceptionFn) {
		if (isComplete()) {
			try {
				if (exception == null) {
					fn.accept(result);
				} else {
					exceptionFn.accept(exception);
				}
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				try {
					if (e == null) {
						fn.accept(result);
					} else {
						exceptionFn.accept(e);
					}
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(result, e);
			}

			@Override
			public String describe() {
				return ".whenComplete(" + formatToString(fn) + ", " + formatToString(exceptionFn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenComplete(RunnableEx action) {
		if (isComplete()) {
			try {
				action.run();
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				try {
					action.run();
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(result, e);
			}

			@Override
			public String describe() {
				return ".whenComplete(" + formatToString(action) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenResult(ConsumerEx<? super T> fn) {
		if (isComplete()) {
			if (isResult()) {
				try {
					fn.accept(result);
				} catch (Exception ex) {
					handleError(ex, this);
					return Promise.ofException(ex);
				}
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					try {
						fn.accept(result);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					complete(result);
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".whenResult(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenResult(RunnableEx action) {
		if (isComplete()) {
			if (isResult()) {
				try {
					action.run();
				} catch (Exception ex) {
					handleError(ex, this);
					return Promise.ofException(ex);
				}
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					try {
						action.run();
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					complete(result);
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".whenResult(" + formatToString(action) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenException(ConsumerEx<Exception> fn) {
		if (isComplete()) {
			if (isException()) {
				try {
					fn.accept(exception);
				} catch (Exception ex) {
					handleError(ex, this);
					return Promise.ofException(ex);
				}
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					complete(result);
				} else {
					try {
						fn.accept(e);
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".whenException(" + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public <E extends Exception> Promise<T> whenException(Class<E> clazz, ConsumerEx<? super E> fn) {
		if (isComplete()) {
			try {
				if (exception != null && clazz.isAssignableFrom(exception.getClass())) {
					fn.accept((E) exception);
				}
				return this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result1, @Nullable Exception e) {
				try {
					if (e != null && clazz.isAssignableFrom(e.getClass())) {
						fn.accept((E) e);
					}
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(result1, e);
			}

			@Override
			public String describe() {
				return ".whenException(" + clazz.getName() + ", " + formatToString(fn) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenException(RunnableEx action) {
		if (isComplete()) {
			if (isException()) {
				try {
					action.run();
				} catch (Exception ex) {
					handleError(ex, this);
					return Promise.ofException(ex);
				}
			}
			return this;
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					complete(result);
				} else {
					try {
						action.run();
					} catch (Exception ex) {
						handleError(ex, this);
						completeExceptionally(ex);
						return;
					}
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".whenException(" + formatToString(action) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<T> whenException(Class<? extends Exception> clazz, RunnableEx action) {
		if (isComplete()) {
			try {
				if (exception != null && clazz.isAssignableFrom(exception.getClass())) {
					action.run();
				}
				return this;
			} catch (Exception ex) {
				handleError(ex, this);
				return Promise.ofException(ex);
			}
		}
		NextPromise<T, T> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				try {
					if (e != null && clazz.isAssignableFrom(e.getClass())) {
						action.run();
					}
				} catch (Exception ex) {
					handleError(ex, this);
					completeExceptionally(ex);
					return;
				}
				complete(result, e);
			}

			@Override
			public String describe() {
				return ".whenException(" + clazz.getName() + ", " + formatToString(action) + ')';
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	private static final Object NO_RESULT = new Object();

	@Override
	public Promise<T> async() {
		if (isComplete()) {
			SettablePromise<T> promise = new SettablePromise<>();
			getCurrentReactor().post(runnableOf(promise,
					exception == null ?
							() -> promise.set(result) :
							() -> promise.setException(exception)));
			return promise;
		}
		return this;
	}

	@Override
	public <U, V> Promise<V> combine(Promise<? extends U> other, BiFunctionEx<? super T, ? super U, ? extends V> fn) {
		if (this.isComplete()) {
			if (this.isResult()) {
				return (Promise<V>) other
						.map(otherResult -> fn.apply(this.getResult(), otherResult))
						.whenException(() -> Recyclers.recycle(this.getResult()));
			}
			other.whenResult(Recyclers::recycle);
			return (Promise<V>) this;
		}
		if (other.isComplete()) {
			if (other.isResult()) {
				return (Promise<V>) this
						.map(result -> fn.apply(result, other.getResult()))
						.whenException(() -> Recyclers.recycle(other.getResult()));
			}
			this.whenResult(Recyclers::recycle);
			return (Promise<V>) other;
		}
		PromiseCombine<T, V, U> resultPromise = new PromiseCombine<>(fn);
		other.subscribe(resultPromise::acceptOther);
		next0(resultPromise);
		return resultPromise;
	}

	@SuppressWarnings({"unchecked", "WeakerAccess"})
	public static class PromiseCombine<T, V, U> extends NextPromise<T, V> {
		final BiFunctionEx<? super T, ? super U, ? extends V> fn;
		@Nullable T thisResult = (T) NO_RESULT;
		@Nullable U otherResult = (U) NO_RESULT;

		PromiseCombine(BiFunctionEx<? super T, ? super U, ? extends V> fn) {
			this.fn = fn;
		}

		@Override
		public void acceptNext(T result, @Nullable Exception e) {
			if (e == null) {
				if (otherResult != NO_RESULT) {
					onBothResults(result, otherResult);
				} else {
					thisResult = result;
				}
			} else {
				onAnyException(e);
			}
		}

		public void acceptOther(U result, @Nullable Exception e) {
			if (e == null) {
				if (thisResult != NO_RESULT) {
					onBothResults(thisResult, result);
				} else {
					otherResult = result;
				}
			} else {
				onAnyException(e);
			}
		}

		void onBothResults(@Nullable T thisResult, @Nullable U otherResult) {
			try {
				tryComplete(fn.apply(thisResult, otherResult));
			} catch (Exception e) {
				handleError(e, fn);
				tryCompleteExceptionally(e);
			}
		}

		void onAnyException(Exception e) {
			if (tryCompleteExceptionally(e)) {
				if (thisResult != NO_RESULT) Recyclers.recycle(thisResult);
				if (otherResult != NO_RESULT) Recyclers.recycle(otherResult);
			}
		}

		@Override
		public String describe() {
			return ".combine(" + formatToString(fn) + ')';
		}
	}

	@Override
	public Promise<Void> both(Promise<?> other) {
		if (this.isComplete()) {
			if (this.isResult()) {
				Recyclers.recycle(this.getResult());
				return other.map(AbstractPromise::recycleToVoid);
			}
			other.whenResult(Recyclers::recycle);
			return (Promise<Void>) this;
		}
		if (other.isComplete()) {
			if (other.isResult()) {
				Recyclers.recycle(other.getResult());
				return this.map(AbstractPromise::recycleToVoid);
			}
			this.whenResult(Recyclers::recycle);
			return (Promise<Void>) other;
		}
		PromiseBoth<Object> resultPromise = new PromiseBoth<>();
		other.next(resultPromise);
		next0(resultPromise);
		return resultPromise;
	}

	protected static @Nullable Void recycleToVoid(Object item) {
		Recyclers.recycle(item);
		return null;
	}

	public static class PromiseBoth<T> extends NextPromise<T, Void> {
		int counter = 2;

		@Override
		public void acceptNext(T result, @Nullable Exception e) {
			if (e == null) {
				Recyclers.recycle(result);
				if (--counter == 0) {
					complete(null);
				}
			} else {
				tryCompleteExceptionally(e);
			}
		}

		@Override
		public String describe() {
			return ".both()";
		}
	}

	@Override
	public Promise<T> either(Promise<? extends T> other) {
		if (isComplete()) {
			if (isResult()) {
				other.whenResult(Recyclers::recycle);
				return this;
			}
			return (Promise<T>) other;
		}
		if (other.isComplete()) {
			if (other.isResult()) {
				this.whenResult(Recyclers::recycle);
				return (Promise<T>) other;
			}
			return this;
		}
		EitherPromise<T> resultPromise = new EitherPromise<>();
		other.next(resultPromise);
		next0(resultPromise);
		return resultPromise;
	}

	public static final class EitherPromise<T> extends NextPromise<T, T> {
		int errors = 2;

		@Override
		public void acceptNext(T result, @Nullable Exception e) {
			if (e == null) {
				if (!tryComplete(result)) {
					Recyclers.recycle(result);
				}
			} else {
				if (--errors == 0) {
					completeExceptionally(new Exception("Both promises completed exceptionally"));
				}
			}
		}

		@Override
		public String describe() {
			return ".either()";
		}
	}

	@Override
	public Promise<Try<T>> toTry() {
		if (isComplete()) {
			return Promise.of(isResult() ? Try.of(result) : Try.ofException(exception));
		}
		NextPromise<T, Try<T>> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					complete(Try.of(result));
				} else {
					complete(Try.ofException(e));
				}
			}

			@Override
			public String describe() {
				return ".toTry()";
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public Promise<Void> toVoid() {
		if (isComplete()) {
			return isResult() ? Promise.complete() : (Promise<Void>) this;
		}
		NextPromise<T, Void> resultPromise = new NextPromise<>() {
			@Override
			public void acceptNext(T result, @Nullable Exception e) {
				if (e == null) {
					complete(null);
				} else {
					completeExceptionally(e);
				}
			}

			@Override
			public String describe() {
				return ".toVoid()";
			}
		};
		next0(resultPromise);
		return resultPromise;
	}

	@Override
	public void next(NextPromise<? super T, ?> cb) {
		if (isComplete()) {
			cb.acceptNext(result, exception);
			return;
		}
		next0(cb);
	}

	@Async.Schedule
	private void next0(NextPromise<? super T, ?> callback) {
		assert !isComplete();

		if (next == null) {
			next = callback;
		} else if (next instanceof Object[] array) {
			array = Arrays.copyOf(array, array.length + 1);
			array[array.length - 1] = callback;
			next = array;
		} else {
			Object[] array = new Object[2];
			array[0] = next;
			array[1] = callback;
			next = array;
		}
	}

	@Override
	public Promise<T> subscribe(Callback<? super T> cb) {
		if (isComplete()) {
			cb.accept(result, exception);
			return this;
		}
		if (cb instanceof NextPromise) {
			//type of `cb` matters
			//noinspection FunctionalExpressionCanBeFolded
			cb = cb::accept;
		}
		if (next == null) {
			next = cb;
		} else if (next instanceof Object[] array) {
			array = Arrays.copyOf(array, array.length + 1);
			array[array.length - 1] = cb;
			next = array;
		} else {
			Object[] array = new Object[2];
			array[0] = next;
			array[1] = cb;
			next = array;
		}
		return this;
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		if (isComplete()) {
			if (isResult()) {
				return CompletableFuture.completedFuture(result);
			} else {
				CompletableFuture<T> future = new CompletableFuture<>();
				future.completeExceptionally(exception);
				return future;
			}
		}
		CompletableFuture<T> future = new CompletableFuture<>();
		subscribe(new Callback<>() {
			@Override
			public void accept(T result, @Nullable Exception e) {
				if (e == null) {
					future.complete(result);
				} else {
					future.completeExceptionally(e);
				}
			}

			@Override
			public String toString() {
				return ".toCompletableFuture()";
			}
		});
		return future;
	}

	private static final String INDENT = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
	private static final Pattern PACKAGE_NAME_AND_LAMBDA_PART = Pattern.compile("^(?:" + INDENT + "\\.)*((?:" + INDENT + "?)\\$\\$Lambda\\$\\d+)/.*$");

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	private static <T> void appendChildren(StringBuilder sb, Object callback, String indent) {
		if (callback == null) {
			return;
		}
		if (callback instanceof Object[] nextCallbacks) {
			for (Object nextCallback : nextCallbacks) {
				appendChildren(sb, nextCallback, indent);
				if (nextCallback == null) break;
			}
		} else {
			indent += "\t";
			sb.append("\n");
			if (callback instanceof AbstractPromise) {
				sb.append(((AbstractPromise<T>) callback).toString(indent));
			} else {
				sb.append(indent + callback);
			}
		}
	}

	private static String formatToString(Object object) {
		return PACKAGE_NAME_AND_LAMBDA_PART.matcher(object.toString()).replaceAll("$1");
	}

	private String toString(String indent) {
		StringBuilder sb = new StringBuilder(indent);
		sb.append(describe());
		if (isComplete()) {
			sb.append('{');
			if (exception == null) {
				sb.append(result);
			} else {
				sb.append("exception=");
				sb.append(exception.getClass().getSimpleName());
			}
			sb.append('}');
		}
		appendChildren(sb, next, indent);
		return sb.toString();
	}

	protected String describe() {
		return "AbstractPromise";
	}

	@Override
	public String toString() {
		return toString("");
	}
}
