package io.activej.async.function;

import io.activej.promise.SettableCallback;

@FunctionalInterface
public interface CallbackSupplierEx<R> {
	void get(SettableCallback<R> cb) throws Exception;
}
