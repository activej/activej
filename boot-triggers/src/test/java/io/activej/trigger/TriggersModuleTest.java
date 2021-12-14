package io.activej.trigger;

import io.activej.common.ref.RefBoolean;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.launcher.LauncherService;
import io.activej.trigger.Triggers.TriggerWithResult;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPool.Instances;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TriggersModuleTest {

	@Test
	public void testDuplicatesRejection() {
		TriggersModule triggersModule = TriggersModule.create()
				.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create());

		try {
			triggersModule.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create());
			fail();
		} catch (IllegalArgumentException e) {
			assertEquals("Cannot assign duplicate trigger", e.getMessage());
		}
	}

	@Test
	public void testWithSeveralWorkerPools() throws Exception {
		int firstPoolSize = 10;
		int secondPoolSize = 5;
		Injector injector = Injector.of(
				WorkerPoolModule.create(),
				new AbstractModule() {
					int counter = 0;

					@Provides
					@Named("first")
					WorkerPool firstPool(WorkerPools workerPools) {
						return workerPools.createPool(firstPoolSize);
					}

					@Provides
					@Named("second")
					WorkerPool secondPool(WorkerPools workerPools) {
						return workerPools.createPool(secondPoolSize);
					}

					@Provides
					@Worker
					String worker() {
						return "" + counter++;
					}

					@Provides
					Integer provide(@Named("first") WorkerPool workerPool1, @Named("second") WorkerPool workerPool2) {
						workerPool1.getInstances(String.class);
						workerPool2.getInstances(String.class);
						return 0;
					}
				},
				TriggersModule.create()
						.with(String.class, Severity.HIGH, "test", s -> TriggerResult.create())
		);
		injector.getInstance(Key.of(WorkerPool.class, "first")).getInstances(String.class);
		injector.getInstance(Key.of(WorkerPool.class, "second")).getInstances(String.class);
		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}
		RefBoolean wasExecuted = new RefBoolean(false);
		try {
			Triggers triggersWatcher = injector.getInstance(Triggers.class);
			assertEquals(firstPoolSize + secondPoolSize, triggersWatcher.getResults().size());
			triggersWatcher.getResults()
					.forEach(triggerWithResult -> assertTrue(triggerWithResult.toString().startsWith("HIGH : String : test :: ")));
			wasExecuted.set(true);
		} finally {
			assertTrue(wasExecuted.get());
		}
	}

	@Test
	public void testPresentOptionalDependency() throws Exception {
		Injector injector = Injector.of(
				new AbstractModule() {
					@Provides
					OptionalDependency<TestClass> optionalTestClass() {
						return OptionalDependency.of(new TestClass());
					}
				},
				TriggersModule.create()
		);

		OptionalDependency<TestClass> optional = injector.getOptionalDependency(TestClass.class);
		assertTrue(optional.isPresent());

		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}

		Triggers triggers = injector.getInstance(Triggers.class);
		List<TriggerWithResult> results = triggers.getResults();
		assertEquals(1, results.size());
		TriggerWithResult result = results.get(0);

		Trigger trigger = result.getTrigger();
		assertEquals(TestClass.class.getSimpleName(), trigger.getComponent());
		assertEquals("test", trigger.getName());
		assertSame(Severity.HIGH, trigger.getSeverity());
		assertTrue(result.getTriggerResult().isPresent());
	}

	@Test
	public void testSyntheticOptionalDependency() throws Exception {
		Injector injector = Injector.of(
				new AbstractModule() {
					@Override
					protected void configure() {
						bindOptionalDependency(TestClass.class);
					}

					@Provides
					TestClass testClass() {
						return new TestClass();
					}
				},
				TriggersModule.create()
		);

		OptionalDependency<TestClass> optional = injector.getOptionalDependency(TestClass.class);
		assertTrue(optional.isPresent());

		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}

		Triggers triggers = injector.getInstance(Triggers.class);
		List<TriggerWithResult> results = triggers.getResults();
		assertEquals(1, results.size());
		TriggerWithResult result = results.get(0);

		Trigger trigger = result.getTrigger();
		assertEquals(TestClass.class.getSimpleName(), trigger.getComponent());
		assertEquals("test", trigger.getName());
		assertSame(Severity.HIGH, trigger.getSeverity());
		assertTrue(result.getTriggerResult().isPresent());
	}

	@Test
	public void testMissingOptionalDependency() throws Exception {
		Injector injector = Injector.of(
				new AbstractModule() {
					@Provides
					OptionalDependency<TestClass> optionalTestClass() {
						return OptionalDependency.empty();
					}
				},
				TriggersModule.create()
		);

		OptionalDependency<TestClass> optional = injector.getOptionalDependency(TestClass.class);
		assertFalse(optional.isPresent());

		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}

		Triggers triggers = injector.getInstance(Triggers.class);
		List<TriggerWithResult> results = triggers.getResults();
		assertTrue(results.isEmpty());
	}

	@Test
	public void testPresentOptionalDependencyWithSeveralWorkerPools() throws Exception {
		int firstPoolSize = 10;
		int secondPoolSize = 5;
		Injector injector = Injector.of(
				WorkerPoolModule.create(),
				new AbstractModule() {

					@Provides
					@Named("first")
					WorkerPool firstPool(WorkerPools workerPools) {
						return workerPools.createPool(firstPoolSize);
					}

					@Provides
					@Named("second")
					WorkerPool secondPool(WorkerPools workerPools) {
						return workerPools.createPool(secondPoolSize);
					}

					@Provides
					@Worker
					OptionalDependency<TestClass> optionalTestClass() {
						return OptionalDependency.of(new TestClass());
					}
				},
				TriggersModule.create()
		);
		Instances<OptionalDependency<TestClass>> firstPoolInstances = injector.getInstance(Key.of(WorkerPool.class, "first"))
				.getInstances(new Key<OptionalDependency<TestClass>>() {});

		assertEquals(firstPoolSize, firstPoolInstances.size());
		for (OptionalDependency<TestClass> optional : firstPoolInstances) {
			assertTrue(optional.isPresent());
		}

		Instances<OptionalDependency<TestClass>> secondPoolInstances = injector.getInstance(Key.of(WorkerPool.class, "second"))
				.getInstances(new Key<OptionalDependency<TestClass>>() {});

		assertEquals(secondPoolSize, secondPoolInstances.size());
		for (OptionalDependency<TestClass> optional : secondPoolInstances) {
			assertTrue(optional.isPresent());
		}

		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}

		List<TriggerWithResult> results = injector.getInstance(Triggers.class).getResults();
		assertEquals(firstPoolSize + secondPoolSize, results.size());
		results.forEach(result -> {
			Trigger trigger = result.getTrigger();
			assertEquals(TestClass.class.getSimpleName(), trigger.getComponent());
			assertEquals("test", trigger.getName());
			assertSame(Severity.HIGH, trigger.getSeverity());
			assertTrue(result.getTriggerResult().isPresent());
		});
	}

	@Test
	public void testSyntheticOptionalDependencyWithSeveralWorkerPools() throws Exception {
		int firstPoolSize = 10;
		int secondPoolSize = 5;
		Injector injector = Injector.of(
				WorkerPoolModule.create(),
				new AbstractModule() {

					@Override
					protected void configure() {
						bind(new Key<OptionalDependency<TestClass>>() {}).in(Worker.class);
					}

					@Provides
					@Named("first")
					WorkerPool firstPool(WorkerPools workerPools) {
						return workerPools.createPool(firstPoolSize);
					}

					@Provides
					@Named("second")
					WorkerPool secondPool(WorkerPools workerPools) {
						return workerPools.createPool(secondPoolSize);
					}

					@Provides
					@Worker
					TestClass testClass() {
						return new TestClass();
					}
				},
				TriggersModule.create()
		);
		Instances<OptionalDependency<TestClass>> firstPoolInstances = injector.getInstance(Key.of(WorkerPool.class, "first"))
				.getInstances(new Key<OptionalDependency<TestClass>>() {});

		assertEquals(firstPoolSize, firstPoolInstances.size());
		for (OptionalDependency<TestClass> optional : firstPoolInstances) {
			assertTrue(optional.isPresent());
		}

		Instances<OptionalDependency<TestClass>> secondPoolInstances = injector.getInstance(Key.of(WorkerPool.class, "second"))
				.getInstances(new Key<OptionalDependency<TestClass>>() {});

		assertEquals(secondPoolSize, secondPoolInstances.size());
		for (OptionalDependency<TestClass> optional : secondPoolInstances) {
			assertTrue(optional.isPresent());
		}

		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}

		List<TriggerWithResult> results = injector.getInstance(Triggers.class).getResults();
		assertEquals(firstPoolSize + secondPoolSize, results.size());
		results.forEach(result -> {
			Trigger trigger = result.getTrigger();
			assertEquals(TestClass.class.getSimpleName(), trigger.getComponent());
			assertEquals("test", trigger.getName());
			assertSame(Severity.HIGH, trigger.getSeverity());
			assertTrue(result.getTriggerResult().isPresent());
		});
	}

	private static class TestClass implements HasTriggers {

		@Override
		public void registerTriggers(TriggerRegistry registry) {
			registry.add(Severity.HIGH, "test", TriggerResult::create);
		}
	}
}
