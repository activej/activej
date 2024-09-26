package io.activej.test;

import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.service.Service;
import io.activej.service.ServiceGraphModule;
import io.activej.test.ActiveJServiceRunnerTest.TestModule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

@RunWith(ActiveJServiceRunner.class)
@UseModules({TestModule.class})
public class ActiveJServiceRunnerTest {

	private static class TestService implements Service {
		private boolean starting;
		private boolean stopped;

		@Override
		public CompletableFuture<?> start() {
			assertFalse(starting);
			this.starting = true;
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<?> stop() {
			assertFalse(stopped);
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
		assertTrue(service instanceof TestService);
		assertFalse(((TestService) service).starting);
		assertFalse(((TestService) service).stopped);
	}

	@Test
	public void test(Service service, @Named("SecondInstance") Service second) {
		Assert.assertSame(service, serviceCopy);
		Assert.assertNotSame(service, second);

		assertTrue(service instanceof TestService);
		assertTrue(((TestService) service).starting);
		assertFalse(((TestService) service).stopped);

		assertTrue(second instanceof TestService);
		assertTrue(((TestService) second).starting);
		assertFalse(((TestService) second).stopped);
	}

	@After
	public void after(Service service) {
		assertTrue(service instanceof TestService);
		assertTrue(((TestService) service).starting);
		assertTrue(((TestService) service).stopped);
	}

	@RunWith(ActiveJServiceRunner.class)
	public static class WithoutModulesTest {
		@Test
		public void test() {
			assertTrue(true);
		}
	}

	@RunWith(ActiveJServiceRunner.class)
	public static class BeforeModulesTest {
		private Service testService;

		@Before
		@UseModules({TestModule.class})
		public void before(Service service) {
			assertTrue(service instanceof TestService);
			assertFalse(((TestService) service).starting);
			assertFalse(((TestService) service).stopped);
			testService = service;
		}

		@Test
		public void test() {
			assertNotNull(testService);
			assertTrue(testService instanceof TestService);
			assertTrue(((TestService) testService).starting);
			assertFalse(((TestService) testService).stopped);
		}
	}

	public static class OptionalDependencyTestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		@Named("service 1")
		OptionalDependency<Service> service1() {
			return OptionalDependency.of(new TestService());
		}

		@Provides
		@Named("service 2")
		OptionalDependency<Service> service2(@Named("service 1") OptionalDependency<Service> service1) {
			return OptionalDependency.empty();
		}

		@Provides
		@Eager
		@Named("service 3")
		OptionalDependency<Service> service3(@Named("service 1") OptionalDependency<Service> service1, @Named("service 2") OptionalDependency<Service> service2) {
			return OptionalDependency.of(new TestService());
		}
	}

	@RunWith(ActiveJServiceRunner.class)
	public static class OptionalModulesTest {
		private OptionalDependency<Service> optional1;
		private OptionalDependency<Service> optional2;
		private OptionalDependency<Service> optional3;

		@Before
		@UseModules({OptionalDependencyTestModule.class})
		public void before(@Named("service 1") OptionalDependency<Service> optional1, @Named("service 2") OptionalDependency<Service> optional2, @Named("service 3") OptionalDependency<Service> optional3) {
			assertNotNull(optional1);
			assertTrue(optional1.isPresent());
			Service service1 = optional1.get();
			assertTrue(service1 instanceof TestService);

			assertFalse(((TestService) service1).starting);
			assertFalse(((TestService) service1).stopped);

			this.optional1 = optional1;

			assertNotNull(optional2);
			assertFalse(optional2.isPresent());

			this.optional2 = optional2;

			assertNotNull(optional3);
			assertTrue(optional3.isPresent());
			Service service3 = optional3.get();
			assertTrue(service3 instanceof TestService);

			assertFalse(((TestService) service3).starting);
			assertFalse(((TestService) service3).stopped);

			this.optional3 = optional3;
		}

		@Test
		public void test() {
			assertNotNull(optional1);
			assertTrue(optional1.isPresent());
			Service service1 = optional1.get();
			assertTrue(service1 instanceof TestService);

			assertTrue(((TestService) service1).starting);
			assertFalse(((TestService) service1).stopped);

			assertNotNull(optional2);
			assertFalse(optional2.isPresent());

			assertNotNull(optional3);
			assertTrue(optional3.isPresent());
			Service service3 = optional3.get();
			assertTrue(service3 instanceof TestService);

			assertTrue(((TestService) service3).starting);
			assertFalse(((TestService) service3).stopped);
		}

		@After
		public void after() {
			assertNotNull(optional1);
			assertTrue(optional1.isPresent());
			Service service1 = optional1.get();
			assertTrue(service1 instanceof TestService);

			assertTrue(((TestService) service1).starting);
			assertTrue(((TestService) service1).stopped);

			assertNotNull(optional2);
			assertFalse(optional2.isPresent());

			assertNotNull(optional3);
			assertTrue(optional3.isPresent());
			Service service3 = optional3.get();
			assertTrue(service3 instanceof TestService);

			assertTrue(((TestService) service3).starting);
			assertTrue(((TestService) service3).stopped);
		}
	}

	public static class OptionalDependencyTestModule2 extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		@Named("service 1")
		Service service1() {
			return new TestService();
		}

		@Provides
		@Named("service 2")
		Service service2(@Named("service 1") OptionalDependency<Service> service1) {
			return new TestService();
		}

		@Provides
		@Eager
		@Named("service 3")
		Service service3(@Named("service 1") Service service1, @Named("service 2") OptionalDependency<Service> service2) {
			return new TestService();
		}
	}

	@RunWith(ActiveJServiceRunner.class)
	public static class OptionalModulesTest2 {
		private Service service1;
		private Service service2;
		private Service service3;

		@Before
		@UseModules({OptionalDependencyTestModule2.class})
		public void before(@Named("service 1") Service service1, @Named("service 2") Service service2, @Named("service 3") Service service3) {
			assertTrue(service1 instanceof TestService);
			assertFalse(((TestService) service1).starting);
			assertFalse(((TestService) service1).stopped);
			this.service1 = service1;

			assertTrue(service2 instanceof TestService);
			assertFalse(((TestService) service2).starting);
			assertFalse(((TestService) service2).stopped);
			this.service2 = service2;

			assertTrue(service3 instanceof TestService);
			assertFalse(((TestService) service3).starting);
			assertFalse(((TestService) service3).stopped);
			this.service3 = service3;
		}

		@Test
		public void test() {
			assertTrue(((TestService) service1).starting);
			assertFalse(((TestService) service1).stopped);

			assertTrue(((TestService) service2).starting);
			assertFalse(((TestService) service2).stopped);

			assertTrue(((TestService) service2).starting);
			assertFalse(((TestService) service2).stopped);
		}

		@After
		public void after() {
			assertTrue(((TestService) service1).starting);
			assertTrue(((TestService) service1).stopped);

			assertTrue(((TestService) service2).starting);
			assertTrue(((TestService) service2).stopped);

			assertTrue(((TestService) service3).starting);
			assertTrue(((TestService) service3).stopped);
		}
	}
}
