package io.activej.async.callback;

@FunctionalInterface
public interface CallbackFunctionEx<T, R> {
	void apply(T t, Callback<R> cb) throws Exception;
}
