package io.activej;

import io.activej.di.Injector;
import io.activej.di.Key;
import io.activej.di.module.Module;
import io.activej.di.module.ModuleBuilder;
import io.activej.di.module.OptionalGeneratorModule;
import io.activej.http.di.PromiseGeneratorModule;
import io.activej.promise.Promise;
import org.junit.Test;

import java.util.Optional;

import static junit.framework.TestCase.*;

/**
 * @since 3.0.0
 */
public final class GeneratorsTest {
	@Test
	public void promiseGeneratorTest() {
		Module module = ModuleBuilder.create()
				.bind(String.class).to(() -> "Hello, World")
				.bind(new Key<Promise<String>>() {})
				.build();

		Module module1 = ModuleBuilder.create()
				.install(module)
				.install(PromiseGeneratorModule.create())
				.build();

		Injector injector = Injector.of(module1);
		Promise<String> instance = injector.getInstance(new Key<Promise<String>>() {});
		assertEquals("Hello, World", instance.getResult());
		assertFalse(injector.hasBinding(new Key<Promise<Integer>>() {}));
	}

	@Test
	public void optionalGeneratorTest() {
		Module module = ModuleBuilder.create()
				.bind(String.class).to(() -> "Hello, World")
				.bind(new Key<Optional<String>>() {})
				.build();

		Module module1 = ModuleBuilder.create()
				.install(module)
				.install(OptionalGeneratorModule.create())
				.build();

		Injector injector = Injector.of(module1);
		Optional<String> instance = injector.getInstance(new Key<Optional<String>>() {});
		assertTrue(instance.isPresent());
		assertEquals("Hello, World", instance.get());
	}
}
