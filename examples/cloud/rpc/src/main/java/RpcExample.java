import io.activej.common.initializer.Initializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.AsyncRpcClient;
import io.activej.rpc.client.RpcClient_Reactive;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;
import io.activej.service.ServiceGraphModuleSettings;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.rpc.client.sender.RpcStrategies.server;

//[START EXAMPLE]
public class RpcExample extends Launcher {
	private static final int SERVICE_PORT = 34765;

	@Inject
	private AsyncRpcClient client;

	@Inject
	private RpcServer server;

	@Inject
	private Reactor reactor;

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	RpcServer rpcServer(NioReactor reactor) {
		return RpcServer.create(reactor)
				.withMessageTypes(String.class)
				.withHandler(String.class,
						request -> Promise.of("Hello " + request))
				.withListenPort(SERVICE_PORT);
	}

	@Provides
	AsyncRpcClient rpcClient(NioReactor reactor) {
		return RpcClient_Reactive.create(reactor)
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress(SERVICE_PORT)));
	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts client only after it started the server
		return settings -> settings.addDependency(Key.of(AsyncRpcClient.class), Key.of(RpcServer.class));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		CompletableFuture<Object> future = reactor.submit(() ->
				client.sendRequest("World", 1000)
		);
		System.out.printf("%nRPC result: %s %n%n", future.get());
	}

	public static void main(String[] args) throws Exception {
		RpcExample example = new RpcExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
