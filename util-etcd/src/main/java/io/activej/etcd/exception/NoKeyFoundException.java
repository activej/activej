package io.activej.etcd.exception;

import io.etcd.jetcd.ByteSequence;

public class NoKeyFoundException extends EtcdDataException{
	public NoKeyFoundException(ByteSequence key) {
		super("No key: " + key);
	}
}
