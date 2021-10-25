import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.ModuleBuilder;

import java.util.Optional;

/**
 * @since 3.0.0
 */
//[START REGION_1]
public class OptionalGeneratorModuleExample {
	public static void main(String[] args) {
		Injector injector = Injector.of(ModuleBuilder.create()
				.install(new OptionalGeneratorModule())
				.bind(String.class).toInstance("Hello, World")
				.bind(new Key<Optional<String>>() {})
				.build());
		Optional<String> instance = injector.getInstance(new Key<Optional<String>>() {});
		System.out.println(instance);
	}

	/**
	 * Extension module.
	 * <p>
	 * A binding of <code>Optional&lt;T&gt;</code> for any type <code>T</code> is generated,
	 * with the resulting optional being empty if no binding for <code>T</code> was bound
	 * or containing an instance of <code>T</code>
	 */
	public static final class OptionalGeneratorModule extends AbstractModule {

		@Override
		protected void configure() {
			generate(Optional.class, (bindings, scope, key) -> {
				Binding<?> binding = bindings.get(key.getTypeParameter(0));
				return binding != null ?
						binding.mapInstance(Optional::of) :
						Binding.toInstance(Optional.empty());
			});
		}
	}

}
//[END REGION_1]
