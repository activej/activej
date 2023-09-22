package io.activej.etcd.exception;

public class EtcdException extends Exception {
	public EtcdException(String message) {
		super(message);
	}

	public EtcdException(String message, Throwable cause) {
		super(message, cause);
	}
}
