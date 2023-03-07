import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessageSerializer;
import io.activej.rpc.server.RpcServer;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;

// [START EXAMPLE]
public class ServerModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	NioReactor reactor() {
		return Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.build();
	}

	@Provides
	KeyValueStore keyValueStore() {
		return new KeyValueStore();
	}

	@Provides
	RpcServer rpcServer(NioReactor reactor, KeyValueStore store) {
		return RpcServer.builder(reactor)
				.withSerializer(RpcMessageSerializer.of(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class))
				.withHandler(PutRequest.class, req -> Promise.of(new PutResponse(store.put(req.key(), req.value()))))
				.withHandler(GetRequest.class, req -> Promise.of(new GetResponse(store.get(req.key()))))
				.withListenPort(RPC_SERVER_PORT)
				.build();
	}
}
// [END EXAMPLE]
