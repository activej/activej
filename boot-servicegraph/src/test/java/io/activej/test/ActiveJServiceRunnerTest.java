package io.activej.test;

import io.activej.di.annotation.Named;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.service.Service;
import io.activej.service.ServiceGraphModule;
import io.activej.test.ActiveJServiceRunnerTest.TestModule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

@RunWith(ActiveJServiceRunner.class)
@UseModules({TestModule.class})
public class ActiveJServiceRunnerTest {

	private static class TestService implements Service {
		private boolean starting;
		private boolean stopped;

		@Override
		public CompletableFuture<?> start() {
			this.starting = true;
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<?> stop() {
			this.stopped = true;
			return CompletableFuture.completedFuture(null);
		}
	}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			bind(Service.class).toInstance(new TestService());
			install(ServiceGraphModule.create());
		}

		@Provides
		@Named("SecondInstance")
		Service service() {
			return new TestService();
		}
	}

	private Service serviceCopy;

	@Before
	public void before(Service service) {
		serviceCopy = service;
		Assert.assertTrue(service instanceof TestService);
		Assert.assertFalse(((TestService) service).starting);
		Assert.assertFalse(((TestService) service).stopped);
	}

	@Test
	public void test(Service service, @Named("SecondInstance") Service second) {
		Assert.assertSame(service, serviceCopy);
		Assert.assertNotSame(service, second);

		Assert.assertTrue(service instanceof TestService);
		Assert.assertTrue(((TestService) service).starting);
		Assert.assertFalse(((TestService) service).stopped);

		Assert.assertTrue(second instanceof TestService);
		Assert.assertTrue(((TestService) second).starting);
		Assert.assertFalse(((TestService) second).stopped);
	}

	@After
	public void after(Service service) {
		Assert.assertTrue(service instanceof TestService);
		Assert.assertTrue(((TestService) service).starting);
		Assert.assertTrue(((TestService) service).stopped);
	}

	@RunWith(ActiveJServiceRunner.class)
	public static class WithoutModulesTest {
		@Test
		public void test() {
			Assert.assertTrue(true);
		}
	}

	@RunWith(ActiveJServiceRunner.class)
	public static class BeforeModulesTest {
		private Service testService;

		@Before
		@UseModules({TestModule.class})
		public void before(Service service) {
			Assert.assertTrue(service instanceof TestService);
			Assert.assertFalse(((TestService) service).starting);
			Assert.assertFalse(((TestService) service).stopped);
			testService = service;
		}

		@Test
		public void test() {
			Assert.assertNotNull(testService);
			Assert.assertTrue(testService instanceof TestService);
			Assert.assertTrue(((TestService) testService).starting);
			Assert.assertFalse(((TestService) testService).stopped);
		}
	}
}
