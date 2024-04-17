package io.activej.worker;

import io.activej.common.ref.RefInt;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Inject;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.ModuleBuilder;
import io.activej.worker.annotation.Worker;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkerPoolTest {
	private WorkerPool first;
	private WorkerPool second;
	private WorkerPools pools;

	@Before
	public void setUp() {
		RefInt counter = new RefInt(0);
		Injector injector = Injector.of(
			new AbstractModule() {
				@Override
				protected void configure() {
					bind(String.class).in(Worker.class).to(() -> "String: " + counter.value++);
				}
			},
			WorkerPoolModule.create());

		pools = injector.getInstance(WorkerPools.class);
		first = pools.createPool(4);
		second = pools.createPool(10);
	}

	@Test
	public void addedWorkerPools() {
		assertEquals(Set.of(first, second), new HashSet<>(pools.getWorkerPools()));
	}

	@Test
	public void numberOfInstances() {
		assertEquals(4, first.getInstances(String.class).size());
		assertEquals(10, second.getInstances(String.class).size());
	}

	@Test
	public void numberOfCalls() {
		Set<String> actual = new HashSet<>();
		actual.addAll(first.getInstances(String.class).getList());
		actual.addAll(second.getInstances(String.class).getList());
		Set<String> expected = IntStream.range(0, 14).mapToObj(i -> "String: " + i).collect(toSet());
		assertEquals(expected, actual);
	}

	@Test
	public void injectAnnotationInSingletonScope() {
		Injector injector = Injector.of(
			WorkerPoolModule.create(),
			ModuleBuilder.create()
				.bind(WorkerPool.class).to(workerPools -> workerPools.createPool(4), WorkerPools.class)
				.bind(Id.class)
				.bind(new Key<WorkerPool.Instances<Integer>>() {})
				.bind(Integer.class).to(id -> id.id, Id.class).in(Worker.class)
				.build()
		);

		WorkerPool.Instances<Integer> instance = injector.getInstance(new Key<>() {});
		List<Integer> list = instance.getList();

		assertEquals(4, list.size());
		assertTrue(list.stream().allMatch(id -> id == 1));
	}

	public static final class Id {
		private static int counter = 0;

		private final int id = ++counter;

		@Inject
		public Id() {
		}
	}
}
