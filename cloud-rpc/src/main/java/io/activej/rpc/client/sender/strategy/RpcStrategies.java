package io.activej.rpc.client.sender.strategy;

import io.activej.common.annotation.StaticFactories;
import io.activej.rpc.client.sender.strategy.impl.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.ToIntFunction;

import static io.activej.common.Checks.checkArgument;

@StaticFactories(RpcStrategy.class)
public final class RpcStrategies {
	public static RpcStrategy server(InetSocketAddress address) {
		return new Server(address);
	}

	public static List<? extends RpcStrategy> servers(InetSocketAddress... addresses) {
		return servers(List.of(addresses));
	}

	public static List<? extends RpcStrategy> servers(List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return addresses.stream()
				.map(Server::new)
				.toList();
	}

	public static RpcStrategy firstAvailable(RpcStrategy... strategies) {
		return firstAvailable(List.of(strategies));
	}

	public static RpcStrategy firstAvailable(List<? extends RpcStrategy> strategies) {
		return new FirstAvailable(strategies);
	}

	public static RpcStrategy firstValidResult(RpcStrategy... strategies) {
		return firstValidResult(List.of(strategies));
	}

	public static RpcStrategy firstValidResult(List<? extends RpcStrategy> strategies) {
		return FirstValidResult.create(strategies);
	}

	public static RpcStrategy roundRobin(RpcStrategy... strategies) {
		return roundRobin(List.of(strategies));
	}

	public static RpcStrategy roundRobin(List<? extends RpcStrategy> strategies) {
		return RoundRobin.create(strategies);
	}

	public static <T> RpcStrategy sharding(ToIntFunction<T> shardingFunction, RpcStrategy... strategies) {
		return sharding(shardingFunction, List.of(strategies));
	}

	public static <T> RpcStrategy sharding(ToIntFunction<T> shardingFunction, List<? extends RpcStrategy> strategies) {
		return Sharding.create(shardingFunction, strategies);
	}
}
