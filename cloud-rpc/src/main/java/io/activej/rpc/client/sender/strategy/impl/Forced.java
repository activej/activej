package io.activej.rpc.client.sender.strategy.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.client.sender.RpcSender;
import io.activej.rpc.client.sender.strategy.RpcStrategy;

import java.net.InetSocketAddress;
import java.util.Set;

@ExposedInternals
public final class Forced implements RpcStrategy {
	public final RpcStrategy strategy;

	public Forced(RpcStrategy strategy) {this.strategy = strategy;}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return strategy.getAddresses();
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		RpcSender sender = strategy.createSender(pool);
		return sender == null ?
				new RpcClient.NoSenderAvailable() :
				sender;
	}
}
