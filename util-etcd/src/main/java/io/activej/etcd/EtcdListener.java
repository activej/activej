package io.activej.etcd;

import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.Response;

public interface EtcdListener<R> {
	void onNext(Response.Header header, R operation) throws MalformedEtcdDataException;

	void onError(Throwable throwable);

	void onCompleted();
}
