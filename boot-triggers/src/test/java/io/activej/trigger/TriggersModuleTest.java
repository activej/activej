package io.activej.trigger;

import io.activej.common.ref.RefBoolean;
import io.activej.di.Injector;
import io.activej.di.Key;
import io.activej.di.annotation.Named;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.eventloop.Eventloop;
import io.activej.launcher.LauncherService;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static org.junit.Assert.*;

public class TriggersModuleTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testDuplicatesRejection() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot assign duplicate trigger");
		Injector.of(
				TriggersModule.create()
						.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create())
						.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create())
		);
		fail();
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
		injector.getInstance(Key.ofName(WorkerPool.class, "first")).getInstances(String.class);
		injector.getInstance(Key.ofName(WorkerPool.class, "second")).getInstances(String.class);
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
}
