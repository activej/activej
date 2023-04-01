package io.activej.async.function;

import io.activej.promise.SettableCallback;

@FunctionalInterface
public interface CallbackSupplier<R> extends CallbackSupplierEx<R> {
	@Override
	void get(SettableCallback<R> cb);
}
