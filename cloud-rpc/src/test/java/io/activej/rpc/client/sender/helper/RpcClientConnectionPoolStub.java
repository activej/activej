package io.activej.rpc.client.sender.helper;

import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.client.sender.RpcSender;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RpcClientConnectionPoolStub implements RpcClientConnectionPool {
	private final Map<InetSocketAddress, RpcSender> connections = new HashMap<>();

	public void put(InetSocketAddress address, RpcSender connection) {
		connections.put(address, connection);
	}

	public void remove(InetSocketAddress address) {
		connections.remove(address);
	}

	@Override
	public RpcSender get(@NotNull InetSocketAddress address) {
		return connections.get(address);
	}
}
