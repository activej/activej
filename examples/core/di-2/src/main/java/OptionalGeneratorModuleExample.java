import io.activej.di.Injector;
import io.activej.di.Key;
import io.activej.di.module.ModuleBuilder;
import io.activej.di.module.OptionalGeneratorModule;

import java.util.Optional;

/**
 * @since 3.0.0
 */
//[START REGION_1]
public class OptionalGeneratorModuleExample {
	public static void main(String[] args) {
		Injector injector = Injector.of(ModuleBuilder.create()
				.install(OptionalGeneratorModule.create())
				.bind(String.class).toInstance("Hello, World")
				.bind(new Key<Optional<String>>() {})
				.build());
		Optional<String> instance = injector.getInstance(new Key<Optional<String>>() {});
		System.out.println(instance);
	}
}
//[END REGION_1]
