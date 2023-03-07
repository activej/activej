import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.IRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcMessageSerializer;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.rpc.client.sender.strategy.RpcStrategies.server;

// [START EXAMPLE]
public class ClientModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	NioReactor reactor() {
		return Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.withCurrentThread()
				.build();
	}

	@Provides
	IRpcClient rpcClient(NioReactor reactor) {
		return RpcClient.builder(reactor)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializer(RpcMessageSerializer.of(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class))
				.withStrategy(server(new InetSocketAddress("localhost", RPC_SERVER_PORT)))
				.build();
	}
}
// [END EXAMPLE]
