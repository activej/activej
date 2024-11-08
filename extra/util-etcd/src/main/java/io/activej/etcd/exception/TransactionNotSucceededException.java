package io.activej.etcd.exception;

public class TransactionNotSucceededException extends TransactionException{
	public TransactionNotSucceededException() {
		super("Transaction not succeeded");
	}
}
