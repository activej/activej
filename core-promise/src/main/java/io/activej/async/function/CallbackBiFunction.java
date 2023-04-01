package io.activej.async.function;

import io.activej.promise.SettableCallback;

@FunctionalInterface
public interface CallbackBiFunction<T, U, R> extends CallbackBiFunctionEx<T, U, R> {
	@Override
	void apply(T t, U u, SettableCallback<R> cb);
}
