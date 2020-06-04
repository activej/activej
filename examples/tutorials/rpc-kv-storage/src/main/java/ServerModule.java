import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerBuilder;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;

// [START EXAMPLE]
public class ServerModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());
	}

	@Provides
	KeyValueStore keyValueStore() {
		return new KeyValueStore();
	}

	@Provides
	RpcServer rpcServer(Eventloop eventloop, KeyValueStore store) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create())
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withHandler(PutRequest.class, req -> Promise.of(new PutResponse(store.put(req.getKey(), req.getValue()))))
				.withHandler(GetRequest.class, req -> Promise.of(new GetResponse(store.get(req.getKey()))))
				.withListenPort(RPC_SERVER_PORT);
	}
}
// [END EXAMPLE]
