package io.activej.service;

import io.activej.di.Injector;
import io.activej.di.Key;
import io.activej.di.annotation.Named;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import org.hamcrest.core.IsSame;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ServiceGraphTest {
	@Rule
	public final ExpectedException expected = ExpectedException.none();

	@Test
	public void testProperClosingForFailingServiceOneComponent() throws Exception {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.ofName(BlockingService.class, "TopService1"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);
		expected.expectCause(IsSame.sameInstance(FailingModule.INTERRUPTED));
		graph.startFuture().get();
	}

	@Test
	public void testProperClosingForFailingServiceTwoComponents() throws Exception {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.ofName(BlockingService.class, "TopService1"));
		injector.getInstance(Key.ofName(BlockingService.class, "TopService2"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);
		expected.expectCause(IsSame.sameInstance(FailingModule.INTERRUPTED));
		graph.startFuture().get();
	}

	// region modules
	public static class FailingModule extends AbstractModule {
		public static final io.activej.common.exception.ExpectedException INTERRUPTED = new io.activej.common.exception.ExpectedException("interrupted");

		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		@Named("FailService")
		BlockingService failService() {
			return new BlockingServiceEmpty() {
				@Override
				public void start() throws Exception{
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
	// endregion
}
