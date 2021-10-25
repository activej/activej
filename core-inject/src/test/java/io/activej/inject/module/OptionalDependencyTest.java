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
		Key<OptionalDependency<String>> key = new Key<OptionalDependency<String>>() {};

		String expected = "string";
		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(String.class).to(() -> expected)
						.bind(key)
						.build());

		OptionalDependency<String> optional = injector.getInstance(key);

		assertTrue(optional.isPresent());

		String string = optional.get();

		assertEquals(expected, string);
	}

	@Test
	public void optionalDependencyEmpty() {
		Key<OptionalDependency<String>> key = new Key<OptionalDependency<String>>() {};

		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(key)
						.build());

		OptionalDependency<String> optional = injector.getInstance(key);

		assertFalse(optional.isPresent());
	}

	@Test
	public void optionalDependencyTransitive() {
		Key<OptionalDependency<String>> key = new Key<OptionalDependency<String>>() {};

		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(Integer.class).to(() -> 123)
						.bind(String.class).to(Object::toString, Integer.class)
						.bind(key)
						.build());

		OptionalDependency<String> optional = injector.getInstance(key);

		assertTrue(optional.isPresent());
		assertEquals("123", optional.get());
	}

	@Test
	public void optionalDependencyTransitiveMissing() {
		Key<OptionalDependency<String>> key = new Key<OptionalDependency<String>>() {};

		Module module = ModuleBuilder.create()
				.bind(String.class).to(Object::toString, Integer.class)
				.bind(key)
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

		Key<OptionalDependency<InstanceProvider<String>>> key = new Key<OptionalDependency<InstanceProvider<String>>>() {};

		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(String.class).to(() -> "str_" + mut.incrementAndGet())
						.bind(key)
						.build());

		OptionalDependency<InstanceProvider<String>> optional = injector.getInstance(key);

		assertTrue(optional.isPresent());

		InstanceProvider<String> provider = optional.get();

		assertEquals("str_1", provider.get());
		assertEquals("str_1", provider.get());
	}
}
