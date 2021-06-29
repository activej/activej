package io.activej.rpc.client.sender;

import io.activej.rpc.client.RpcClientConnectionPool;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

public interface RpcStrategyList {
	List<RpcSender> listOfSenders(RpcClientConnectionPool pool);

	List<RpcSender> listOfNullableSenders(RpcClientConnectionPool pool);

	DiscoveryService getDiscoveryService();

	int size();

	RpcStrategy get(int index);

	static RpcStrategyList ofDiscoveryService(DiscoveryService discoveryService) {
		return DiscoverableRpcStrategyList.create(discoveryService);
	}

	static RpcStrategyList ofAddresses(@NotNull List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return new RpcStrategyListFinal(addresses.stream()
				.map(RpcStrategySingleServer::create)
				.collect(toList()));
	}

	static RpcStrategyList ofStrategies(List<? extends RpcStrategy> strategies) {
		return new RpcStrategyListFinal(new ArrayList<>(strategies));
	}
}
