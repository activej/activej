package io.activej.etcd;

import io.activej.common.exception.MalformedDataException;
import io.etcd.jetcd.Response;

public interface Listener<R> {
	void onNext(Response.Header header, R operation) throws MalformedDataException;

	void onError(Throwable throwable);

	void onCompleted();
}
