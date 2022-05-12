package io.activej.inject.module;

import io.activej.inject.Injector;
import io.activej.inject.InstanceProvider;
import io.activej.inject.Key;
import io.activej.inject.binding.DIException;
import io.activej.inject.binding.OptionalDependency;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class OptionalDependencyTest {
	@Test
	public void optionalDependency() {
		String expected = "string";
		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(String.class).to(() -> expected)
						.bindOptionalDependency(String.class)
						.build());

		OptionalDependency<String> optional = injector.getOptionalDependency(String.class);

		assertTrue(optional.isPresent());

		String string = optional.get();

		assertEquals(expected, string);
	}

	@Test
	public void optionalDependencyEmpty() {
		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bindOptionalDependency(String.class)
						.build());

		OptionalDependency<String> optional = injector.getOptionalDependency(String.class);

		assertFalse(optional.isPresent());
	}

	@Test
	public void optionalDependencyTransitive() {
		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(Integer.class).to(() -> 123)
						.bind(String.class).to(Object::toString, Integer.class)
						.bindOptionalDependency(String.class)
						.build());

		OptionalDependency<String> optional = injector.getOptionalDependency(String.class);

		assertTrue(optional.isPresent());
		assertEquals("123", optional.get());
	}

	@Test
	public void optionalDependencyTransitiveMissing() {
		Module module = ModuleBuilder.create()
				.bind(String.class).to(Object::toString, Integer.class)
				.bindOptionalDependency(String.class)
				.build();
		try {
			Injector.of(module);
			fail();
		} catch (DIException e) {
			assertTrue(e.getMessage().startsWith("Unsatisfied dependencies detected"));
			assertTrue(e.getMessage().contains("key Integer required to make"));
		}
	}

	@Test
	public void optionalDependencyInstanceProvider() {
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(String.class).to(() -> "str_" + mut.incrementAndGet())
						.bindOptionalDependency(new Key<InstanceProvider<String>>() {})
						.build());

		OptionalDependency<InstanceProvider<String>> optional = injector.getOptionalDependency(new Key<>() {});

		assertTrue(optional.isPresent());

		InstanceProvider<String> provider = optional.get();

		assertEquals("str_1", provider.get());
		assertEquals("str_1", provider.get());
	}

	@Test
	public void sameInstance() {
		AtomicInteger created = new AtomicInteger();

		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(String.class).to(() -> "str_" + created.incrementAndGet())
						.bindOptionalDependency(String.class)
						.build());

		String string = injector.getInstance(String.class);
		OptionalDependency<String> stringOpt = injector.getOptionalDependency(String.class);

		assertEquals("str_1", string);
		assertEquals("str_1", stringOpt.get());
	}
}
