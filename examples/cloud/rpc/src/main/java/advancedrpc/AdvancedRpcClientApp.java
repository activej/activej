package advancedrpc;

import io.activej.di.annotation.Inject;
import io.activej.di.module.Module;
import io.activej.di.module.ModuleBuilder;
import io.activej.eventloop.Eventloop;
import io.activej.launcher.Launcher;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.stream.IntStream.range;

public class AdvancedRpcClientApp extends Launcher {
	@Inject
	RpcClient client;

	@Inject
	Eventloop eventloop;

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
		CompletableFuture<Void> future = eventloop.submit(() ->
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
