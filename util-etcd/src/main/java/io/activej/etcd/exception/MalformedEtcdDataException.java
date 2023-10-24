package io.activej.etcd.exception;

public class MalformedEtcdDataException extends EtcdDataException {
	public MalformedEtcdDataException(String message) {
		super(message);
	}

	public MalformedEtcdDataException(String message, Throwable cause) {
		super(message, cause);
	}
}
