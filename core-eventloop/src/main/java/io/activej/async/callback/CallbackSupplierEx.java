package io.activej.async.callback;

@FunctionalInterface
public interface CallbackSupplierEx<R> {
	void get(Callback<R> cb) throws Exception;
}
