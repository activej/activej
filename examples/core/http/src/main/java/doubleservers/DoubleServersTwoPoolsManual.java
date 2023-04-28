package doubleservers;

import io.activej.eventloop.Eventloop;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.module.AbstractModule;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.reactor.nio.NioReactor;
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
 * <p>
 * This example is the same as {@link DoubleServersTwoPools} with an exception that
 * all the bindings in this example are configured manually,
 * rather than using annotations and reflection
 */
public final class DoubleServersTwoPoolsManual extends Launcher {

	PrimaryServer primaryServerFirst;
	PrimaryServer primaryServerSecond;

	@Override
	protected void onInit(Injector injector) {
		this.primaryServerFirst = injector.getInstance(Key.of(PrimaryServer.class, "First"));
		this.primaryServerSecond = injector.getInstance(Key.of(PrimaryServer.class, "Second"));
	}

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

		@Override
		protected void configure() {
			bind(NioReactor.class, "First").to(Eventloop::create);
			bind(NioReactor.class).to(Eventloop::create).in(WorkerFirst.class);
			bind(PrimaryServer.class, "First")
					.to((primaryReactor, workerServers) -> PrimaryServer.builder(primaryReactor, workerServers)
									.withListenAddresses(new InetSocketAddress("localhost", port))
									.build(),
							Key.of(NioReactor.class, "First"), new Key<WorkerPool.Instances<HttpServer>>("First") {});
			bind(HttpServer.class)
					.to((reactor, workerId) -> HttpServer.builder(reactor, request -> HttpResponse.Builder.ok200()
											.withPlainText("Hello from the first server, worker #" + workerId)
											.build())
									.build(),
							Key.of(NioReactor.class), Key.of(int.class, WorkerId.class)).in(WorkerFirst.class);
			bind(WorkerPool.class, "First")
					.to(workerPools -> workerPools.createPool(Scope.of(WorkerFirst.class), WORKERS), WorkerPools.class);
		}
	}

	public static final class SecondModule extends AbstractModule {
		private static final int WORKERS = 5;

		private final int port;

		public SecondModule(int port) {
			this.port = port;
		}

		@Override
		protected void configure() {
			bind(NioReactor.class, "Second").to(Eventloop::create);
			bind(NioReactor.class).to(Eventloop::create).in(WorkerSecond.class);
			bind(PrimaryServer.class, "Second")
					.to((primaryReactor, workerServers) -> PrimaryServer.builder(primaryReactor, workerServers)
									.withListenAddresses(new InetSocketAddress("localhost", port))
									.build(),
							Key.of(NioReactor.class, "Second"), new Key<WorkerPool.Instances<HttpServer>>("Second") {});
			bind(HttpServer.class)
					.to((reactor, workerId) -> HttpServer.builder(reactor, request -> HttpResponse.Builder.ok200()
											.withPlainText("Hello from the second server, worker #" + workerId)
											.build())
									.build(),
							Key.of(NioReactor.class), Key.of(int.class, WorkerId.class)).in(WorkerSecond.class);
			bind(WorkerPool.class, "Second")
					.to(workerPools -> workerPools.createPool(Scope.of(WorkerSecond.class), WORKERS), WorkerPools.class);
		}
	}

}
