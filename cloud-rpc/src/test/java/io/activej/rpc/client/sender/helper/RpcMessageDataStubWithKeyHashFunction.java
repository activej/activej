package io.activej.rpc.client.sender.helper;

import io.activej.rpc.hash.HashFunction;

public final class RpcMessageDataStubWithKeyHashFunction implements HashFunction<Object> {

	@Override
	public int hashCode(Object item) {
		return ((RpcMessageDataStubWithKey) item).getKey();
	}
}
