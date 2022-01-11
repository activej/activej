package discovery;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.EventloopService;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategy;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;

public final class RpcStrategyService<K extends Comparable<K>, S, P> implements EventloopService {
	private final Eventloop eventloop;
	private final DiscoveryService<K, S, P> discoveryService;
	private final Function<P, RpcStrategy> strategyResolver;
	private final Function<Object, K> keyGetter;

	private RpcClient rpcClient;

	private boolean stopped;

	private RpcStrategyService(Eventloop eventloop, DiscoveryService<K, S, P> discoveryService,
			Function<P, RpcStrategy> strategyResolver, Function<Object, K> keyGetter) {
		this.eventloop = eventloop;
		this.discoveryService = discoveryService;
		this.strategyResolver = strategyResolver;
		this.keyGetter = keyGetter;
	}

	public static <K extends Comparable<K>, S, P> RpcStrategyService<K, S, P> create(Eventloop eventloop, DiscoveryService<K, S, P> discoveryService,
			Function<P, RpcStrategy> strategyResolver, Function<Object, K> keyGetter) {
		return new RpcStrategyService<>(eventloop, discoveryService, strategyResolver, keyGetter);
	}

	public void setRpcClient(RpcClient rpcClient) {
		checkState(this.rpcClient == null && rpcClient.getEventloop() == eventloop);

		this.rpcClient = rpcClient;
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		checkNotNull(rpcClient);

		AsyncSupplier<DiscoveryService.Partitionings<K, S, P>> discoverySupplier = discoveryService.discover();
		return discoverySupplier.get()
				.whenResult(partitionings -> {
					RpcStrategy rpcStrategy = partitionings.createRpcStrategy(strategyResolver, keyGetter);
					rpcClient.withStrategy(rpcStrategy);
					Promises.repeat(() ->
							discoverySupplier.get()
									.map((newPartitionings, e) -> {
										if (stopped) return false;
										if (e == null) {
											RpcStrategy newRpcStrategy = newPartitionings.createRpcStrategy(strategyResolver, keyGetter);
											rpcClient.changeStrategy(newRpcStrategy, true);
										}
										return true;
									})
					);
				});

	}

	@Override
	public @NotNull Promise<?> stop() {
		this.stopped = true;
		return Promise.complete();
	}
}
