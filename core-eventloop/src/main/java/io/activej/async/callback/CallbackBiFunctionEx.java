package io.activej.async.callback;

@FunctionalInterface
public interface CallbackBiFunctionEx<T, U, R> {
	void apply(T t, U u, Callback<R> cb) throws Exception;
}
