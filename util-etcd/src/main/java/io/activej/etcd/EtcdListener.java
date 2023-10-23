package io.activej.etcd;

import io.activej.etcd.exception.MalformedEtcdDataException;

public interface EtcdListener<R> {
	void onConnectionEstablished();

	void onNext(long revision, R operation) throws MalformedEtcdDataException;

	void onError(Throwable throwable);

	void onCompleted();
}
