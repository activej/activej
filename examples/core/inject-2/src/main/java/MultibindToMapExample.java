import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;

import java.util.HashMap;
import java.util.Map;

public final class MultibindToMapExample {
	public static void main(String[] args) {
		//[START MODULES]
		Key<Map<Integer, String>> mapKey = new Key<>() {};

		Module module1 = ModuleBuilder.create()
				.bind(mapKey).to(() -> {
					Map<Integer, String> map = new HashMap<>();
					map.put(1, "one");
					map.put(2, "two");
					map.put(3, "three");
					return map;
				})
				.build();

		Module module2 = ModuleBuilder.create()
				.bind(mapKey).to(() -> {
					Map<Integer, String> map = new HashMap<>();
					map.put(4, "four");
					map.put(5, "five");
					map.put(6, "six");
					return map;
				})
				.build();
		//[END MODULES]

		//[START MULTIBINDER]
		Module multibinderModule = ModuleBuilder.create()
				.multibindToMap(Integer.class, String.class)
				.build();
		//[END MULTIBINDER]

		//[START INJECTOR]
		Injector injector = Injector.of(module1, module2, multibinderModule);
		System.out.println(injector.getInstance(mapKey));
		//[END INJECTOR]
	}
}
