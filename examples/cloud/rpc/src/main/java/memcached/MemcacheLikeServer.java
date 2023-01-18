package memcached;

import io.activej.config.Config;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.launcher.Launcher;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;

//[START REGION_1]
public class MemcacheLikeServer extends Launcher {
	@Inject
	WorkerPool.Instances<RpcServer> instances;

	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(3);
	}

	@Provides
	Config config() {
		return Config.builder()
				.with("memcache.buffers", "4")
				.with("memcache.bufferCapacity", "64mb")
				.build();
	}

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
				.install(ServiceGraphModule.create())
				.install(MemcacheMultiServerModule.create())
				.install(WorkerPoolModule.create())
				.build();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		MemcacheLikeServer server = new MemcacheLikeServer();
		server.launch(args);
	}
}
//[END REGION_1]
