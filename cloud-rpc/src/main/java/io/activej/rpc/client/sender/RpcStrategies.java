package io.activej.rpc.client.sender;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.ToIntFunction;

import static io.activej.common.Checks.checkArgument;

public class RpcStrategies {
	public static RpcStrategy server(InetSocketAddress address) {
		return new RpcStrategy_SingleServer(address);
	}

	public static List<? extends RpcStrategy> servers(InetSocketAddress... addresses) {
		return servers(List.of(addresses));
	}

	public static List<? extends RpcStrategy> servers(List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return addresses.stream()
				.map(RpcStrategy_SingleServer::new)
				.toList();
	}

	public static RpcStrategy firstAvailable(RpcStrategy... strategies) {
		return firstAvailable(List.of(strategies));
	}

	public static RpcStrategy firstAvailable(List<? extends RpcStrategy> strategies) {
		return new RpcStrategy_FirstAvailable(strategies);
	}

	public static RpcStrategy firstValidResult(RpcStrategy... strategies) {
		return firstValidResult(List.of(strategies));
	}

	public static RpcStrategy firstValidResult(List<? extends RpcStrategy> strategies) {
		return RpcStrategy_FirstValidResult.create(strategies);
	}

	public static RpcStrategy roundRobin(RpcStrategy... strategies) {
		return roundRobin(List.of(strategies));
	}

	public static RpcStrategy roundRobin(List<? extends RpcStrategy> strategies) {
		return RpcStrategy_RoundRobin.create(strategies);
	}

	public static <T> RpcStrategy sharding(ToIntFunction<T> shardingFunction, RpcStrategy... strategies) {
		return sharding(shardingFunction, List.of(strategies));
	}

	public static <T> RpcStrategy sharding(ToIntFunction<T> shardingFunction, List<? extends RpcStrategy> strategies) {
		return RpcStrategy_Sharding.create(shardingFunction, strategies);
	}
}
