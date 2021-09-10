package io.activej.service;

import io.activej.async.service.EventloopService;
import io.activej.common.service.BlockingService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ServiceGraphTest {
	@Test
	public void testProperClosingForFailingServiceOneComponent() {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.of(BlockingService.class, "TopService1"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);

		try {
			graph.startFuture().get();
			fail();
		} catch (Exception e) {
			assertSame(FailingModule.INTERRUPTED, e.getCause());
		}
	}

	@Test
	public void testProperClosingForFailingServiceTwoComponents() {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.of(BlockingService.class, "TopService1"));
		injector.getInstance(Key.of(BlockingService.class, "TopService2"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);

		try {
			graph.startFuture().get();
			fail();
		} catch (Exception e) {
			assertSame(FailingModule.INTERRUPTED, e.getCause());
		}
	}

	@Test
	public void testEventloopServiceStartStopWithRuntimeException() {
		Injector injector = Injector.of(new FailingEventloopModule());
		injector.getInstance(Key.of(EventloopService.class, "start"));
		injector.getInstance(Key.of(EventloopService.class, "stop"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);

		try {
			graph.startFuture().get();
			fail();
		} catch (Exception e) {
			assertSame(FailingEventloopModule.ERROR, e.getCause());
		}

		try {
			graph.stopFuture().get();
			fail();
		} catch (Exception e) {
			assertSame(FailingEventloopModule.ERROR, e.getCause());
		}

	}

	// region modules
	public static class FailingModule extends AbstractModule {

		private static final io.activej.test.ExpectedException INTERRUPTED = new io.activej.test.ExpectedException("interrupted");

		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		@Named("FailService")
		BlockingService failService() {
			return new BlockingServiceEmpty() {
				@Override
				public void start() throws Exception {
					throw INTERRUPTED;
				}
			};
		}

		@Provides
		@Named("TopService1")
		BlockingService service1(@Named("FailService") BlockingService failService) {
			return new BlockingServiceEmpty();
		}

		@Provides
		@Named("TopService2")
		BlockingService service2(@Named("FailService") BlockingService failService) {
			return new BlockingServiceEmpty();
		}
	}

	public static class BlockingServiceEmpty implements BlockingService {
		@Override
		public void start() throws Exception {
		}

		@Override
		public void stop() {
		}
	}

	public static class FailingEventloopModule extends AbstractModule {
		private static final RuntimeException ERROR = new RuntimeException();

		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		Eventloop eventloop() {
			return Eventloop.create();
		}

		@Provides
		@Named("start")
		EventloopService failedStart(Eventloop eventloop) {
			return new EventloopServiceEmpty(eventloop) {
				@Override
				public @NotNull Promise<?> start() {
					throw ERROR;
				}
			};
		}

		@Provides
		@Named("stop")
		EventloopService failStop(Eventloop eventloop) {
			return new EventloopServiceEmpty(eventloop) {
				@Override
				public @NotNull Promise<?> stop() {
					throw ERROR;
				}
			};
		}
	}

	public static class EventloopServiceEmpty implements EventloopService {
		private final Eventloop eventloop;

		public EventloopServiceEmpty(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		@Override
		public @NotNull Eventloop getEventloop() {
			return eventloop;
		}

		@Override
		public @NotNull Promise<?> start() {
			return Promise.complete();
		}

		@Override
		public @NotNull Promise<?> stop() {
			return Promise.complete();
		}
	}
	// endregion
}
