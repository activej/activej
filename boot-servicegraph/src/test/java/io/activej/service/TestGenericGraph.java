package io.activej.service;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.service.adapter.ServiceAdapters.SimpleServiceAdapter;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.junit.Test;

public class TestGenericGraph {
	public static final int WORKERS = 4;

	public static class Pojo {
		private final String object;

		public Pojo(String object) {
			this.object = object;
		}

	}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.create()
					.register(Pojo.class, new SimpleServiceAdapter<Pojo>(false, false) {
						@Override
						protected void start(Pojo instance) {
							System.out.println("...starting " + instance + " : " + instance.object);
						}

						@Override
						protected void stop(Pojo instance) {
							System.out.println("...stopping " + instance + " : " + instance.object);
						}
					}));
			install(WorkerPoolModule.create());
		}

		@Provides
		Pojo integerPojo(WorkerPool workerPool) {
			WorkerPool.Instances<Pojo> list = workerPool.getInstances(Key.of(Pojo.class, "worker"));
			WorkerPool.Instances<Pojo> listOther = workerPool.getInstances(Key.of(Pojo.class, "anotherWorker"));
			return new Pojo("root");
		}

		@Provides
		WorkerPool pool(WorkerPools workerPools) {
			return workerPools.createPool(WORKERS);
		}

		@Provides
		@Worker
		@Named("worker")
		Pojo stringPojoOther() {
			return new Pojo("worker");
		}

		@Provides
		@Worker
		@Named("anotherWorker")
		Pojo stringPojo(@Named("worker") Pojo worker) {
			return new Pojo("anotherWorker");
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Injector.of(new TestModule());
		injector.getInstance(Pojo.class);

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
