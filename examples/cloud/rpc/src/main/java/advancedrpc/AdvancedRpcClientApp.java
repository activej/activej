package advancedrpc;

import io.activej.inject.annotation.Inject;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.launcher.Launcher;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.AsyncRpcClient;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.stream.IntStream.range;

public class AdvancedRpcClientApp extends Launcher {
	@Inject
	AsyncRpcClient client;

	@Inject
	Reactor reactor;

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
				.install(ServiceGraphModule.create())
				.install(AdvancedRpcClientModule.create())
				.build();
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		System.out.println();
		CompletableFuture<Void> future = reactor.submit(() ->
				Promises.all(range(0, 100).mapToObj(i ->
						client.sendRequest(i, 1000)
								.whenResult(res -> System.out.println("Answer : " + res)))));
		future.get();
		System.out.println();
	}

	public static void main(String[] args) throws Exception {
		AdvancedRpcClientApp app = new AdvancedRpcClientApp();
		app.launch(args);
	}
}
