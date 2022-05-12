import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;

import java.util.Set;

public final class MultibindToSetExample {
	public static void main(String[] args) {
		//[START MODULES]
		Key<Set<Integer>> setKey = new Key<>() {};

		Module module1 = ModuleBuilder.create()
				.bind(setKey).toInstance(Set.of(1, 2, 3))
				.build();
		Module module2 = ModuleBuilder.create()
				.bind(setKey).toInstance(Set.of(3, 4, 5))
				.build();
		//[END MODULES]

		//[START MULTIBINDER]
		Module multibinderModule = ModuleBuilder.create()
				.multibindToSet(Integer.class)
				.build();
		//[END MULTIBINDER]

		//[START INJECTOR]
		Injector injector = Injector.of(module1, module2, multibinderModule);
		System.out.println(injector.getInstance(setKey));
		//[END INJECTOR]
	}
}
