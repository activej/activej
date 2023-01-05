package io.activej.launchers.http;

import io.activej.async.service.TaskScheduler;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.http.ReactiveHttpClient;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.jmx.JmxModule;
import io.activej.launcher.LauncherService;
import io.activej.launchers.initializers.Initializers;
import io.activej.net.PrimaryServer;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class ObjectNameRenameTest {

	@Test
	public void testRenaming() throws ExecutionException, InterruptedException, MalformedObjectNameException {
		Injector injector = Injector.of(
				new TestModule(),
				JmxModule.create()
						.withInitializer(Initializers.renamedClassNames(
								Map.of(
										HttpServer.class, "AsyncHttpServer",
										AsyncHttpClient.class, "AsyncHttpClient",
										TaskScheduler.class, "EventloopTaskScheduler",
										Reactor.class, Eventloop.class.getName(),
										NioReactor.class, Eventloop.class.getName()
								))),
				WorkerPoolModule.create()
		);

		injector.createEagerInstances();
		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}

		MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

		assertEquals(1, beanServer.queryNames(new ObjectName("io.activej.eventloop:type=Eventloop"), null).size());
		assertEquals(4, beanServer.queryNames(new ObjectName("io.activej.eventloop:type=Eventloop,scope=Worker,workerId=worker-*"), null).size());

		assertEquals(1, beanServer.queryNames(new ObjectName("io.activej.async.service:type=EventloopTaskScheduler"), null).size());

		assertEquals(1, beanServer.queryNames(new ObjectName("io.activej.http:type=AsyncHttpClient,qualifier=Test"), null).size());
		assertEquals(4, beanServer.queryNames(new ObjectName("io.activej.http:type=AsyncHttpServer,scope=Worker,workerId=worker-*"), null).size());
	}

	private static class TestModule extends AbstractModule {
		@Provides
		WorkerPool workerPool(WorkerPools pools) {
			return pools.createPool(4);
		}

		@Provides
		NioReactor primaryReactor() {
			return Eventloop.create();
		}

		@Provides
		@Worker
		NioReactor workerReactor() {
			return Eventloop.create();
		}

		@Provides
		@Eager
		PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<HttpServer> workerServers) {
			return PrimaryServer.create(primaryReactor, workerServers);
		}

		@Provides
		@Worker
		HttpServer workerServer(NioReactor workerReactor) {
			return HttpServer.create(workerReactor, request -> HttpResponse.ok200());
		}

		@Provides
		@Named("Test")
		@Eager
		AsyncHttpClient httpClient(NioReactor reactor) {
			return ReactiveHttpClient.create(reactor);
		}

		@Provides
		@Eager
		TaskScheduler scheduler(NioReactor reactor) {
			return TaskScheduler.create(reactor, Promise::complete)
					.withSchedule(TaskScheduler.Schedule.immediate());
		}
	}
}
