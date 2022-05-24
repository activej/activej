package specializer;

import io.activej.common.exception.FatalErrorHandler;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;

/**
 * Run {@link ScopedRpcBenchmarkClient} after launching this server
 */
public final class ScopedRpcServerExample extends Launcher {

	public static final int PORT = 9001;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create().withFatalErrorHandler(FatalErrorHandler.rethrow());
	}

	@Provides
	@Eager
	RpcServer rpcServer(Eventloop eventloop, RpcRequestHandler<RpcRequest, RpcResponse> handler) {
		return RpcServer.create(eventloop)
				.withMessageTypes(RpcRequest.class, RpcResponse.class)
				.withHandler(RpcRequest.class, handler)
				.withListenPort(PORT);
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
