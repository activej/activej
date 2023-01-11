package advancedrpc;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverters;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.AsyncRpcClient;
import io.activej.rpc.client.RpcClient_Reactive;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategy_FirstAvailable;
import io.activej.rpc.client.sender.RpcStrategy_RendezvousHashing;
import io.activej.serializer.SerializerBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static io.activej.common.Checks.checkState;
import static io.activej.rpc.client.sender.RpcStrategies.server;

public class AdvancedRpcClientModule extends AbstractModule {
	private AdvancedRpcClientModule() {
	}

	public static AdvancedRpcClientModule create() {
		return new AdvancedRpcClientModule();
	}

	@Provides
	AsyncRpcClient rpcClient(NioReactor reactor, RpcStrategy strategy) {
		return RpcClient_Reactive.create(reactor)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializerBuilder(SerializerBuilder.create())
				.withMessageTypes(Integer.class)
				.withStrategy(strategy);
	}

	@Provides
	RpcStrategy rpcStrategy(Config config) {
		List<InetSocketAddress> inetAddresses = config.get(ConfigConverters.ofList(
				ConfigConverters.ofInetSocketAddress(), ","), "client.addresses");
		checkState(inetAddresses.size() == 4);

		return RpcStrategy_FirstAvailable.create(
				RpcStrategy_RendezvousHashing.create(Object::hashCode)
						.withShard(1, server(inetAddresses.get(0)))
						.withShard(2, server(inetAddresses.get(1))),
				RpcStrategy_RendezvousHashing.create(Object::hashCode)
						.withShard(1, server(inetAddresses.get(2)))
						.withShard(2, server(inetAddresses.get(3)))
		);
	}

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("protocol.compression", "false")
				.with("client.addresses", "localhost:9000, localhost:9001, localhost:9002, localhost:9003")
				.overrideWith(Config.ofSystemProperties("config"));
	}
}
