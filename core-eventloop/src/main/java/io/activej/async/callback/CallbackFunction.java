package io.activej.async.callback;

@FunctionalInterface
public interface CallbackFunction<T, R> extends CallbackFunctionEx<T, R> {
	@Override
	void apply(T t, Callback<R> cb);
}
