package specializer;

import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerFactory;
import io.activej.service.ServiceGraphModule;

import java.util.List;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;

/**
 * Run {@link ScopedRpcBenchmarkClient} after launching this server
 */
public final class ScopedRpcServerExample extends Launcher {

	public static final int PORT = 9001;

	@Provides
	NioReactor reactor() {
		return Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.build();
	}

	@Provides
	@Eager
	RpcServer rpcServer(NioReactor reactor, RpcRequestHandler<RpcRequest, RpcResponse> handler) {
		return RpcServer.builder(reactor)
				.withSerializer(SerializerFactory.builder()
						.withSubclasses(RpcMessage.SUBCLASSES_ID, List.of(RpcRequest.class, RpcResponse.class))
						.build()
						.create(RpcMessage.class))
				.withHandler(RpcRequest.class, handler)
				.withListenPort(PORT)
				.build();
	}

	@Override
	protected Module getModule() {
		return Modules.combine(ServiceGraphModule.create(), CookieBucketModule.create(), RpcScopedModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		/*
		 * You can uncomment a line below to enable binding specialization.
		 * This will lead to increase in ScopedRpcBenchmarkClient results
		 */

//		Injector.useSpecializer(); // Uncomment to speed up the server!

		ScopedRpcServerExample app = new ScopedRpcServerExample();
		app.launch(args);
	}
}
