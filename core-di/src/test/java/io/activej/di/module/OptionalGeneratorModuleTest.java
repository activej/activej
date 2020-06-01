package io.activej.di.module;

import io.activej.di.Injector;
import io.activej.di.InstanceProvider;
import io.activej.di.Key;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class OptionalGeneratorModuleTest {
	@Test
	public void instanceProviderMap() {
		AtomicInteger mut = new AtomicInteger();

		Key<Optional<InstanceProvider<String>>> key = new Key<java.util.Optional<InstanceProvider<String>>>() {};

		Injector injector = Injector.of(
				OptionalGeneratorModule.create(),
				ModuleBuilder.create()
						.bind(String.class).to(() -> "str_" + mut.incrementAndGet())
						.bind(key)
						.build());

		// OptionalGeneratorModule calls mapInstance on provider binding and it causes it to compile
		// an intermediate transient binding for the InstanceProvider
		// and that means that we cannot just ban transient InstanceProviders sadly

		java.util.Optional<InstanceProvider<String>> optional = injector.getInstance(key);

		assertTrue(optional.isPresent());

		InstanceProvider<String> provider = optional.get();

		System.out.println(provider.get());
		System.out.println(provider.get());
	}
}
