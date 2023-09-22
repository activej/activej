package io.activej.etcd.exception;

public class EtcdDataException extends EtcdException {
	public EtcdDataException(String message) {
		super(message);
	}

	public EtcdDataException(String message, Throwable cause) {
		super(message, cause);
	}
}
