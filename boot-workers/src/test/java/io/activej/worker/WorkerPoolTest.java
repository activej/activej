package io.activej.worker;

import io.activej.common.ref.RefInt;
import io.activej.inject.Injector;
import io.activej.inject.module.AbstractModule;
import io.activej.worker.annotation.Worker;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

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
}
