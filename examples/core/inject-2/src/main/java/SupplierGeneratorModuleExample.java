import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.ModuleBuilder;

import java.util.function.Supplier;

/**
 * @since 3.0.0
 */
//[START REGION_1]
public class SupplierGeneratorModuleExample {
	public static void main(String[] args) {
		Injector injector = Injector.of(ModuleBuilder.create()
				.install(new SupplierGeneratorModule())
				.bind(String.class).toInstance("Hello, World")
				.bind(new Key<Supplier<String>>() {})
				.build());
		Supplier<String> supplier = injector.getInstance(new Key<Supplier<String>>() {});
		System.out.println(supplier.get());
	}

	/**
	 * Extension module.
	 * <p>
	 * A binding of <code>Supplier&lt;T&gt;</code> for any type <code>T</code> is generated,
	 * with the resulting supplier returning <code>null</code> if no binding for <code>T</code> was bound
	 * or an instance of <code>T</code>, otherwise
	 */
	public static final class SupplierGeneratorModule extends AbstractModule {

		@Override
		protected void configure() {
			generate(Supplier.class, (bindings, scope, key) -> {
				Binding<?> binding = bindings.get(key.getTypeParameter(0));
				return binding != null ?
						binding.mapInstance(instance -> () -> instance) :
						Binding.toInstance(() -> null);
			});
		}
	}

}
//[END REGION_1]
