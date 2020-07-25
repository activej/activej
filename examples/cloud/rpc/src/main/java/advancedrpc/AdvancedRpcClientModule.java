package advancedrpc;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverters;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.serializer.SerializerBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static io.activej.common.Checks.checkState;
import static io.activej.rpc.client.sender.RpcStrategies.*;

public class AdvancedRpcClientModule extends AbstractModule {
	private AdvancedRpcClientModule() {
	}

	public static AdvancedRpcClientModule create() {
		return new AdvancedRpcClientModule();
	}

	@Provides
	RpcClient rpcClient(Eventloop eventloop, RpcStrategy strategy) {
		return RpcClient.create(eventloop)
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

		return firstAvailable(
				rendezvousHashing(Object::hashCode)
						.withShard(1, server(inetAddresses.get(0)))
						.withShard(2, server(inetAddresses.get(1))),
				rendezvousHashing(Object::hashCode)
						.withShard(1, server(inetAddresses.get(2)))
						.withShard(2, server(inetAddresses.get(3)))
		);
	}

	@Provides
	Eventloop eventloop() {
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
