import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.ReactiveRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.serializer.SerializerBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.common.exception.FatalErrorHandler.rethrow;

// [START EXAMPLE]
public class ClientModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Override
	protected void configure() {
		bind(Reactor.class).to(NioReactor.class);
	}

	@Provides
	NioReactor reactor() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrow())
				.withCurrentThread();
	}

	@Provides
	RpcClient rpcClient(NioReactor reactor) {
		return ReactiveRpcClient.create(reactor)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializerBuilder(SerializerBuilder.create())
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withStrategy(RpcStrategies.server(new InetSocketAddress("localhost", RPC_SERVER_PORT)));
	}
}
// [END EXAMPLE]
