package io.activej.service;

import io.activej.async.service.ReactiveService;
import io.activej.common.service.BlockingService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.ExpectedException;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class ServiceGraphTest {
	@Test
	public void testProperClosingForFailingServiceOneComponent() {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.of(BlockingService.class, "TopService1"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);

		Exception e = assertThrows(Exception.class, () -> graph.startFuture().get());
		assertSame(FailingModule.INTERRUPTED, e.getCause());
	}

	@Test
	public void testProperClosingForFailingServiceTwoComponents() {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.of(BlockingService.class, "TopService1"));
		injector.getInstance(Key.of(BlockingService.class, "TopService2"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);

		Exception e = assertThrows(Exception.class, () -> graph.startFuture().get());
		assertSame(FailingModule.INTERRUPTED, e.getCause());
	}

	@Test
	public void testEventloopServiceStartStopWithRuntimeException() {
		Injector injector = Injector.of(new FailingEventloopModule());
		injector.getInstance(Key.of(ReactiveService.class, "start"));
		injector.getInstance(Key.of(ReactiveService.class, "stop"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);

		Exception startException = assertThrows(Exception.class, () -> graph.startFuture().get());
		assertSame(FailingEventloopModule.ERROR, startException.getCause());

		Exception stopException = assertThrows(Exception.class, () -> graph.stopFuture().get());
		assertSame(FailingEventloopModule.ERROR, stopException.getCause());
	}

	// region modules
	public static class FailingModule extends AbstractModule {

		private static final ExpectedException INTERRUPTED = new ExpectedException("interrupted");

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
		Reactor reactor() {
			return Eventloop.create();
		}

		@Provides
		@Named("start")
		ReactiveService failedStart(Reactor reactor) {
			return new EmptyService(reactor) {
				@Override
				public Promise<?> start() {
					throw ERROR;
				}
			};
		}

		@Provides
		@Named("stop")
		ReactiveService failStop(Reactor reactor) {
			return new EmptyService(reactor) {
				@Override
				public Promise<?> stop() {
					throw ERROR;
				}
			};
		}
	}

	public static class EmptyService implements ReactiveService {
		private final Reactor reactor;

		public EmptyService(Reactor reactor) {
			this.reactor = reactor;
		}

		@Override
		public Reactor getReactor() {
			return reactor;
		}

		@Override
		public Promise<?> start() {
			return Promise.complete();
		}

		@Override
		public Promise<?> stop() {
			return Promise.complete();
		}
	}
	// endregion
}
