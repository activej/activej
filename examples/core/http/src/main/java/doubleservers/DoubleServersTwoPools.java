package doubleservers;

import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpServer;
import io.activej.http.HttpResponse;
import io.activej.inject.Scope;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.WorkerId;

import java.net.InetSocketAddress;

import static io.activej.common.Utils.concat;
import static io.activej.http.HttpUtils.getHttpAddresses;
import static io.activej.inject.module.Modules.combine;

/**
 * This example represents a launcher for 2 multithreaded HTTP servers.
 * <p>
 * We use two {@link WorkerPool}s here of 3 and 5 workers, as the number of workers
 * for each server is different.
 * <p>
 * To do this we use two modules, each providing a different {@link WorkerPool}
 * with a different number of workers. Modules also provide all the dependencies for
 * the respective HTTP servers.
 */
public final class DoubleServersTwoPools extends Launcher {

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
				WorkerPoolModule.create(WorkerFirst.class, WorkerSecond.class),
				new FirstModule(8001),
				new SecondModule(8002)
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
		new DoubleServersTwoPoolsManual().launch(args);
	}

	public static final class FirstModule extends AbstractModule {
		private static final int WORKERS = 3;

		private final int port;

		public FirstModule(int port) {
			this.port = port;
		}

		@Provides
		@Named("First")
		Eventloop primaryEventloop() {
			return Eventloop.create();
		}

		@Provides
		@WorkerFirst
		Eventloop workerEventloop() {
			return Eventloop.create();
		}

		@Provides
		@Named("First")
		PrimaryServer primaryServerFirst(@Named("First") Eventloop primaryEventloop, @Named("First") WorkerPool.Instances<AsyncHttpServer> workerServers) {
			return PrimaryServer.create(primaryEventloop, workerServers)
					.withListenAddresses(new InetSocketAddress("localhost", port));
		}

		@Provides
		@WorkerFirst
		AsyncHttpServer workerServerFirst(Eventloop eventloop, @WorkerId int workerId) {
			return AsyncHttpServer.create(eventloop, request -> HttpResponse.ok200()
					.withPlainText("Hello from the first server, worker #" + workerId));
		}


		@Provides
		@Named("First")
		WorkerPool workerPool(WorkerPools workerPools) {
			return workerPools.createPool(Scope.of(WorkerFirst.class), WORKERS);
		}
	}

	public static final class SecondModule extends AbstractModule {
		private static final int WORKERS = 5;

		private final int port;

		public SecondModule(int port) {
			this.port = port;
		}

		@Provides
		@Named("Second")
		Eventloop primaryEventloop() {
			return Eventloop.create();
		}

		@Provides
		@WorkerSecond
		Eventloop workerEventloop() {
			return Eventloop.create();
		}

		@Provides
		@Named("Second")
		PrimaryServer primaryServerSecond(@Named("Second") Eventloop primaryEventloop, @Named("Second") WorkerPool.Instances<AsyncHttpServer> workerServers) {
			return PrimaryServer.create(primaryEventloop, workerServers)
					.withListenAddresses(new InetSocketAddress("localhost", port));
		}

		@Provides
		@WorkerSecond
		AsyncHttpServer workerServerSecond(Eventloop eventloop, @WorkerId int workerId) {
			return AsyncHttpServer.create(eventloop, request -> HttpResponse.ok200()
					.withPlainText("Hello from the second server, worker #" + workerId));
		}


		@Provides
		@Named("Second")
		WorkerPool workerPool(WorkerPools workerPools) {
			return workerPools.createPool(Scope.of(WorkerSecond.class), WORKERS);
		}
	}

}
