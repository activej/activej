package io.activej.service;

import io.activej.common.service.BlockingService;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Modules;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class OptionalDependencyTest {

	@Test
	public void testWithoutOptionalDependencies() throws ExecutionException, InterruptedException {
		doTest(new AbstractModule() {
			@Provides
			@Named("Service 1")
			TestOptionalService service1() {
				return new TestOptionalService();
			}

			@Provides
			@Named("Service 2")
			TestOptionalService service2() {
				return new TestOptionalService();
			}

			@Provides
			TestOptionalService testService(@Named("Service 1") TestOptionalService ignored) {
				return new TestOptionalService();
			}

			@Provides
			TestInterface testInterface(TestOptionalService service, @Named("Service 2") TestOptionalService ignored) {
				return service;
			}
		});
	}

	@Test
	public void testOptionalDependencies() throws ExecutionException, InterruptedException {
		doTest(new AbstractModule() {
			@Provides
			@Named("Service 1")
			TestOptionalService service1() {
				return new TestOptionalService();
			}

			@Provides
			@Named("Service 2")
			TestOptionalService service2() {
				return new TestOptionalService();
			}

			@Provides
			OptionalDependency<TestOptionalService> testService(@Named("Service 1") TestOptionalService ignored) {
				return OptionalDependency.of(new TestOptionalService());
			}

			@Provides
			TestInterface testInterface(OptionalDependency<TestOptionalService> service, @Named("Service 2") TestOptionalService ignored) {
				return service.isPresent() ? service.get() : new TestInterface() {
					@Override
					public int startCounter() {
						return 0;
					}

					@Override
					public int stopCounter() {
						return 0;
					}
				};
			}
		});
	}

	private void doTest(AbstractModule module2) throws InterruptedException, ExecutionException {
		Injector injector = Injector.of(
				Modules.combine(ServiceGraphModule.create(), module2)
		);

		TestInterface instance = injector.getInstance(TestInterface.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}

		assertEquals(1, instance.startCounter());
		assertEquals(1, instance.stopCounter());

		TestOptionalService service1 = injector.getInstance(Key.of(TestOptionalService.class, "Service 1"));
		TestOptionalService service2 = injector.getInstance(Key.of(TestOptionalService.class, "Service 2"));
		assertEquals(1, service1.startCounter());
		assertEquals(1, service1.stopCounter());
		assertEquals(1, service2.startCounter());
		assertEquals(1, service2.stopCounter());
	}


	public interface TestInterface {
		int startCounter();

		int stopCounter();
	}

	public static class TestOptionalService implements TestInterface, BlockingService {
		private final AtomicInteger startCounter = new AtomicInteger(0);
		private final AtomicInteger stopCounter = new AtomicInteger(0);

		@Override
		public void start() {
			startCounter.incrementAndGet();
		}

		@Override
		public void stop() {
			stopCounter.incrementAndGet();
		}

		@Override
		public int startCounter() {
			return startCounter.get();
		}

		@Override
		public int stopCounter() {
			return stopCounter.get();
		}
	}
}
