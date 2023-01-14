package io.activej.rpc.client.sender;

import java.net.InetSocketAddress;
import java.util.List;

import static io.activej.common.Checks.checkArgument;

public class RpcStrategies {
	public static RpcStrategy server(InetSocketAddress address) {
		return RpcStrategy_SingleServer.of(address);
	}

	public static List<? extends RpcStrategy> servers(InetSocketAddress... addresses) {
		return servers(List.of(addresses));
	}

	public static List<? extends RpcStrategy> servers(List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return addresses.stream()
				.map(RpcStrategy_SingleServer::of)
				.toList();
	}

}
