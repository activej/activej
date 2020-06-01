package io.activej.rpc.client.sender.helper;

import io.activej.async.callback.Callback;
import io.activej.rpc.client.sender.RpcSender;
import org.jetbrains.annotations.NotNull;

public final class RpcSenderStub implements RpcSender {
	private int requests;

	public int getRequests() {
		return requests;
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
		requests++;
	}
}
