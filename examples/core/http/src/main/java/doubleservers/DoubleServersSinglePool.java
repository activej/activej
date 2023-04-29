package doubleservers;

import io.activej.eventloop.Eventloop;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.inject.Key;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

import java.net.InetSocketAddress;

import static io.activej.common.Utils.concat;
import static io.activej.http.HttpUtils.getHttpAddresses;
import static io.activej.inject.module.Modules.combine;

/**
 * This example represents a launcher for 2 multithreaded HTTP servers.
 * <p>
 * We use a single {@link WorkerPool} here of 4 workers, as the number of workers
 * for each server is 4.
 * <p>
 * To do this we use a single module that provides a single default {@link WorkerPool}
 * as well as all the dependencies for "First" and "Second" HTTP servers.
 */
public final class DoubleServersSinglePool extends Launcher {
	private static final int WORKERS = 4;

	@Inject
	@Named("First")
	PrimaryServer primaryServerFirst;

	@Inject
	@Named("Second")
	PrimaryServer primaryServerSecond;

	@Override
	protected io.activej.inject.module.Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				WorkerPoolModule.create(),
				new ServerModule(8001, 8002)
		);
	}

	@Override
	protected void run() throws Exception {
		if (logger.isInfoEnabled()) {
			logger.info("HTTP Servers are now available at {}",
					concat(getHttpAddresses(primaryServerFirst), getHttpAddresses(primaryServerSecond)));
		}
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new DoubleServersSinglePool().launch(args);
	}

	public static final class ServerModule extends AbstractModule {
		private final int firstPort;
		private final int secondPort;

		public ServerModule(int firstPort, int secondPort) {
			this.firstPort = firstPort;
			this.secondPort = secondPort;
		}

		@Provides
		@Named("First")
		NioReactor primaryReactorFirst() {
			return Eventloop.create();
		}

		@Provides
		@Named("Second")
		NioReactor primaryReactorSecond() {
			return Eventloop.create();
		}

		@Provides
		@Worker
		NioReactor workerReactor() {
			return Eventloop.create();
		}

		@Provides
		@Named("First")
		PrimaryServer primaryServerFirst(@Named("First") NioReactor primaryReactor, WorkerPool workerPool) {
			return PrimaryServer.builder(primaryReactor, workerPool.getInstances(Key.of(HttpServer.class, "First")))
					.withListenAddresses(new InetSocketAddress("localhost", firstPort))
					.build();
		}

		@Provides
		@Named("Second")
		PrimaryServer primaryServerSecond(@Named("Second") NioReactor primaryReactor, WorkerPool workerPool) {
			return PrimaryServer.builder(primaryReactor, workerPool.getInstances(Key.of(HttpServer.class, "Second")))
					.withListenAddresses(new InetSocketAddress("localhost", secondPort))
					.build();
		}

		@Provides
		@Worker
		@Named("First")
		HttpServer workerServerFirst(NioReactor reactor, @WorkerId int workerId) {
			return HttpServer.builder(reactor, request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from the first server, worker #" + workerId)
							.toPromise())
					.build();
		}

		@Provides
		@Worker
		@Named("Second")
		HttpServer workerServerSecond(NioReactor reactor, @WorkerId int workerId) {
			return HttpServer.builder(reactor, request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from the second server, worker #" + workerId)
							.toPromise())
					.build();
		}

		@Provides
		WorkerPool workerPool(WorkerPools workerPools) {
			return workerPools.createPool(WORKERS);
		}
	}
}
