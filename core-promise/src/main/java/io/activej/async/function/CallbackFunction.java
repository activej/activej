package io.activej.async.function;

import io.activej.promise.SettableCallback;

@FunctionalInterface
public interface CallbackFunction<T, R> extends CallbackFunctionEx<T, R> {
	@Override
	void apply(T t, SettableCallback<R> cb);
}
