package io.activej.launchers.http;

import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ScopeAnnotation;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.trigger.Severity;
import io.activej.trigger.TriggerResult;
import io.activej.trigger.TriggersModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class ComplexHttpLauncher extends Launcher {
	public static final int SERVER_ONE_PORT = 8081;
	public static final int SERVER_TWO_PORT = 8082;
	public static final int SERVER_THREE_PORT = 8083;

	@ScopeAnnotation
	@Target(METHOD)
	@Retention(RUNTIME)
	public @interface MyWorker {
	}

	// region primary reactors
	@Provides
	NioReactor reactor1() {
		return Eventloop.create();
	}

	@Provides
	@Named("Second")
	NioReactor reactor2() {
		return Eventloop.create();
	}

	@Provides
	@Named("Third")
	NioReactor reactor3() {
		return Eventloop.create();
	}
	// endregion

	// region worker pools
	@Provides
	@Named("First")
	WorkerPool workerPool1(WorkerPools workerPools) {
		return workerPools.createPool(4);
	}

	@Provides
	@Named("Second")
	WorkerPool workerPool2(WorkerPools workerPools) {
		return workerPools.createPool(10);
	}

	@Provides
	@Named("Third")
	WorkerPool workerPool3(WorkerPools workerPools) {
		return workerPools.createPool(Scope.of(MyWorker.class), 4);
	}
	// endregion

	// region primary servers
	@Provides
	@Eager
	@Named("First")
	PrimaryServer server1(NioReactor reactor, @Named("First") WorkerPool.Instances<HttpServer> serverInstances) {
		return PrimaryServer.builder(reactor, serverInstances)
				.withListenAddress(new InetSocketAddress(SERVER_ONE_PORT))
				.build();
	}

	@Provides
	@Eager
	@Named("Second")
	PrimaryServer server2(@Named("Second") NioReactor reactor, @Named("Second") WorkerPool.Instances<HttpServer> serverInstances) {
		return PrimaryServer.builder(reactor, serverInstances)
				.withListenAddress(new InetSocketAddress(SERVER_TWO_PORT))
				.build();
	}

	@Provides
	@Eager
	@Named("Third")
	PrimaryServer server3(@Named("Third") NioReactor reactor, @Named("Third") WorkerPool.Instances<HttpServer> serverInstances) {
		return PrimaryServer.builder(reactor, serverInstances)
				.withListenAddress(new InetSocketAddress(SERVER_THREE_PORT))
				.build();
	}
	// endregion

	// region Worker scope
	@Provides
	@Worker
	NioReactor workerReactor() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	HttpServer workerServer(NioReactor reactor, AsyncServlet servlet) {
		return HttpServer.builder(reactor, servlet).build();
	}

	@Provides
	@Worker
	AsyncServlet workerServlet(@WorkerId int workerId) {
		return $ -> HttpResponse.Builder.ok200()
				.withPlainText("Hello from worker #" + workerId)
				.toPromise();
	}
	// endregion

	// region MyWorker scope
	@Provides
	@MyWorker
	NioReactor myWorkerReactor() {
		return Eventloop.create();
	}

	@Provides
	@MyWorker
	HttpServer myWorkerServer(NioReactor reactor, AsyncServlet servlet) {
		return HttpServer.builder(reactor, servlet).build();
	}

	@Provides
	@MyWorker
	AsyncServlet myWorkerServlet(@WorkerId int workerId) {
		return $ -> HttpResponse.Builder.ok200()
				.withPlainText("Hello from my worker #" + workerId)
				.toPromise();
	}
	// endregion

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				WorkerPoolModule.create(Worker.class, MyWorker.class),
				JmxModule.builder()
						.withScopes(false)
						.build(),
				TriggersModule.builder()
						.with(Key.of(PrimaryServer.class, "First"), Severity.HIGH, "server1", TriggerResult::ofValue)
						.with(Key.of(PrimaryServer.class, "Second"), Severity.HIGH, "server2", TriggerResult::ofValue)
						.with(Key.of(PrimaryServer.class, "Third"), Severity.HIGH, "server3", TriggerResult::ofValue)
						.with(Key.of(Eventloop.class), Severity.HIGH, "eventloop", TriggerResult::ofValue)
						.build()
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		new ComplexHttpLauncher().launch(args);
	}
}


