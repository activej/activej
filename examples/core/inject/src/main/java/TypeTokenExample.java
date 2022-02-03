import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.AbstractModule;
import io.activej.types.Types;

import java.lang.reflect.Type;
import java.util.List;

import static io.activej.types.Types.parameterizedType;

/**
 * A Key class is a type token by itself, so you can construct complex keys with long generics nicely.
 * If you don't like the excess subclassing, you can use {@link Types#parameterizedType(Class, Type...)},
 * as shown in getInstance call.
 */
//[START EXAMPLE]
public final class TypeTokenExample {

	public static void main(String[] args) {
		Injector injector = Injector.of(new AbstractModule() {
			@Override
			protected void configure() {
				bind(String.class).toInstance("hello");
				bind(new Key<List<String>>() {}).to(s -> List.of(s, s, s), String.class);
			}
		});

		Key<List<String>> key = Key.ofType(parameterizedType(List.class, String.class));
		System.out.println(injector.getInstance(key));

		Key<?> complex = Key.ofType(parameterizedType(List.class, parameterizedType(List.class, String.class)));
		Key<List<List<String>>> subclassedButTypesafe = new Key<>() {};
		System.out.println("complex == subclassedButTypesafe = " + complex.equals(subclassedButTypesafe));
	}
}
//[END EXAMPLE]
