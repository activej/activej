import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;

public class BindingTransformationExample extends Launcher {
	//[START REGION_1]
	@Inject
	Person person;
	//[END REGION_1]

	//[START REGION_2]
	@Override
	protected Module getModule() {
		return Modules.combine(
				new PersonModule(),
				new PersonTransformModule()
		);
	}
	//[END REGION_2]

	//[START REGION_3]
	@Override
	protected void run() {
		person.greet();
	}
	//[END REGION_3]

	//[START REGION_4]
	public static void main(String[] args) throws Exception {
		BindingTransformationExample launcher = new BindingTransformationExample();
		launcher.launch(args);
	}
	//[END REGION_4]

	//[START REGION_5]
	public interface Person {
		void greet();
	}
	//[END REGION_5]

	//[START REGION_6]
	public static class PersonModule extends AbstractModule {
		@Provides
		Person greeter() {
			return () -> System.out.println("Hello!");
		}
	}
	//[END REGION_6]

	//[START REGION_7]
	public static class PersonTransformModule extends AbstractModule {
		@Override
		protected void configure() {
			transform(0, (bindings, scope, key, binding) -> {
				if (!Person.class.isAssignableFrom(key.getRawType())) {
					// Ignore any class that is not Person
					return binding;
				}

				return binding.mapInstance(person ->
						(Person) () -> {
							System.out.println("Start of greeting");
							((Person) person).greet();
							System.out.println("End of greeting");
						}
				);
			});
		}
	}
	//[END REGION_7]
}
