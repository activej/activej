package io.activej.async.callback;

@FunctionalInterface
public interface CallbackBiFunction<T, U, R> extends CallbackBiFunctionEx<T, U, R> {
	@Override
	void apply(T t, U u, Callback<R> cb);
}
