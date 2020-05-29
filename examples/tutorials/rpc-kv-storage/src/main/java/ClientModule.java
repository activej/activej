import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.eventloop.Eventloop;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.serializer.SerializerBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.eventloop.FatalErrorHandlers.rethrowOnAnyError;

// [START EXAMPLE]
public class ClientModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError())
				.withCurrentThread();
	}

	@Provides
	RpcClient rpcClient(Eventloop eventloop) {
		return RpcClient.create(eventloop)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializerBuilder(SerializerBuilder.create())
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withStrategy(RpcStrategies.server(new InetSocketAddress("localhost", RPC_SERVER_PORT)));
	}
}
// [END EXAMPLE]
