package io.activej.async.function;

import io.activej.promise.SettableCallback;

@FunctionalInterface
public interface CallbackBiFunctionEx<T, U, R> {
	void apply(T t, U u, SettableCallback<R> cb) throws Exception;
}
