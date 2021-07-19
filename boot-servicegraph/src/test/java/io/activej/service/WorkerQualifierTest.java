package io.activej.service;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.test.rules.ByteBufRule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.service.adapter.ServiceAdapters.combinedAdapter;
import static io.activej.service.adapter.ServiceAdapters.immediateServiceAdapter;

public final class WorkerQualifierTest {
	public static final int WORKERS = 4;

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static class Element1 {}

	public static class Element2 {}

	public static class Element3 {}

	public static class Element4 extends Element1 {}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.create()
					.register(Element4.class, combinedAdapter(
							immediateServiceAdapter(),
							immediateServiceAdapter()))
					.register(Element1.class, immediateServiceAdapter())
					.register(Element2.class, immediateServiceAdapter())
					.register(Element3.class, immediateServiceAdapter()));
			install(WorkerPoolModule.create());
		}

		@Provides
		@Named("Primary")
		Element1 primaryEventloop() {
			return new Element1();
		}

		@Provides
		Element2 primaryServer(@Named("Primary") Element1 primaryEventloop, WorkerPool workerPool) {
			WorkerPool.Instances<Element4> unusedList = workerPool.getInstances(Key.of(Element4.class, "First"));
			return new Element2();
		}

		@Provides
		WorkerPool workerPool(WorkerPools workerPools) {
			return workerPools.createPool(WORKERS);
		}

		@Provides
		@Worker
		@Named("First")
		Element4 ffWorker() {
			return new Element4();
		}

		@Provides
		@Worker
		@Named("Second")
		Element4 fSWorker() {
			return new Element4();
		}

		@Provides
		@Worker
		Element1 workerEventloop() {
			return new Element1();
		}

		@Provides
		@Worker
		Element3 workerHttpServer(Element1 eventloop, @WorkerId int workerId, @Named("Second") Element4 unusedString) {
			return new Element3();
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Injector.of(new TestModule());
		injector.getInstance(Element2.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
