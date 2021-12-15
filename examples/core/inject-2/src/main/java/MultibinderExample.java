import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.Multibinders;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;

public final class MultibinderExample {
	public static void main(String[] args) {
		//[START MULTIBINDER]
		Module multibinderModule = ModuleBuilder.create()
				.multibind(Key.of(Integer.class), Multibinders.ofBinaryOperator(Integer::sum))
				.build();
		//[END MULTIBINDER]

		//[START INTEGERS]
		Module integersModule = ModuleBuilder.create()
				.bind(Integer.class).toInstance(1)
				.bind(Integer.class).toInstance(10)
				.bind(Integer.class).toInstance(100)
				.build();
		//[END INTEGERS]

		//[START INJECTOR]
		Injector injector = Injector.of(multibinderModule, integersModule);
		System.out.println(injector.getInstance(Integer.class));
		//[END INJECTOR]
	}
}
