package io.activej.async.callback;

@FunctionalInterface
public interface CallbackSupplier<R> extends CallbackSupplierEx<R> {
	@Override
	void get(Callback<R> cb);
}
