package advancedrpc;

import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.di.module.ModuleBuilder;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;

public class AdvancedRpcServerApp extends Launcher {
	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(4);
	}

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
				.install(ServiceGraphModule.create())
				.install(WorkerPoolModule.create())
				.install(AdvancedRpcServerModule.create())
				.build();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		AdvancedRpcServerApp app = new AdvancedRpcServerApp();
		app.launch(args);
	}
}
