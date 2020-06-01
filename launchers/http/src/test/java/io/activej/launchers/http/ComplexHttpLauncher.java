package io.activej.launchers.http;

import io.activej.di.Key;
import io.activej.di.Scope;
import io.activej.di.annotation.Eager;
import io.activej.di.annotation.Named;
import io.activej.di.annotation.Provides;
import io.activej.di.annotation.ScopeAnnotation;
import io.activej.di.module.Module;
import io.activej.di.module.Modules;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpServer;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.service.ServiceGraphModule;
import io.activej.trigger.Severity;
import io.activej.trigger.TriggerResult;
import io.activej.trigger.TriggersModule;
import io.activej.worker.*;

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

	// region primary eventloops
	@Provides
	Eventloop eventloop1() {
		return Eventloop.create();
	}

	@Provides
	@Named("Second")
	Eventloop eventloop2() {
		return Eventloop.create();
	}

	@Provides
	@Named("Third")
	Eventloop eventloop3() {
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
	PrimaryServer server1(Eventloop eventloop, @Named("First") WorkerPool.Instances<AsyncHttpServer> serverInstances) {
		return PrimaryServer.create(eventloop, serverInstances)
				.withListenAddress(new InetSocketAddress(SERVER_ONE_PORT));
	}

	@Provides
	@Eager
	@Named("Second")
	PrimaryServer server2(@Named("Second") Eventloop eventloop, @Named("Second") WorkerPool.Instances<AsyncHttpServer> serverInstances) {
		return PrimaryServer.create(eventloop, serverInstances)
				.withListenAddress(new InetSocketAddress(SERVER_TWO_PORT));
	}

	@Provides
	@Eager
	@Named("Third")
	PrimaryServer server3(@Named("Third") Eventloop eventloop, @Named("Third") WorkerPool.Instances<AsyncHttpServer> serverInstances) {
		return PrimaryServer.create(eventloop, serverInstances)
				.withListenAddress(new InetSocketAddress(SERVER_THREE_PORT));
	}
	// endregion

	// region Worker scope
	@Provides
	@Worker
	Eventloop workerEventloop() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	AsyncHttpServer workerServer(Eventloop eventloop, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet);
	}

	@Provides
	@Worker
	AsyncServlet workerServlet(@WorkerId int workerId) {
		return $ -> HttpResponse.ok200().withPlainText("Hello from worker #" + workerId);
	}
	// endregion

	// region MyWorker scope
	@Provides
	@MyWorker
	Eventloop myWorkerEventloop() {
		return Eventloop.create();
	}

	@Provides
	@MyWorker
	AsyncHttpServer myWorkerServer(Eventloop eventloop, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet);
	}

	@Provides
	@MyWorker
	AsyncServlet myWorkerServlet(@WorkerId int workerId) {
		return $ -> HttpResponse.ok200().withPlainText("Hello from my worker #" + workerId);
	}
	// endregion

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				WorkerPoolModule.create(Worker.class, MyWorker.class),
				JmxModule.create()
						.withScopes(false),
				TriggersModule.create()
						.with(Key.ofName(PrimaryServer.class, "First"), Severity.HIGH, "server1", TriggerResult::ofValue)
						.with(Key.ofName(PrimaryServer.class, "Second"), Severity.HIGH, "server2", TriggerResult::ofValue)
						.with(Key.ofName(PrimaryServer.class, "Third"), Severity.HIGH, "server3", TriggerResult::ofValue)
						.with(Key.of(Eventloop.class), Severity.HIGH, "eventloop", TriggerResult::ofValue)
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new ComplexHttpLauncher().launch(args);
	}
}


