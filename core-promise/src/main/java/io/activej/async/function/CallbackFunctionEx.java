package io.activej.async.function;

import io.activej.promise.SettableCallback;

@FunctionalInterface
public interface CallbackFunctionEx<T, R> {
	void apply(T t, SettableCallback<R> cb) throws Exception;
}
