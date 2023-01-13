import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.AsyncRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.serializer.SerializerBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.rpc.client.sender.RpcStrategy.server;

// [START EXAMPLE]
public class ClientModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	NioReactor reactor() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrow())
				.withCurrentThread();
	}

	@Provides
	AsyncRpcClient rpcClient(NioReactor reactor) {
		return RpcClient.create(reactor)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializerBuilder(SerializerBuilder.create())
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withStrategy(server(new InetSocketAddress("localhost", RPC_SERVER_PORT)));
	}
}
// [END EXAMPLE]
