package io.activej.inject.module;

import io.activej.inject.Injector;
import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class BindingOfConstructorTest {

	@Test
	public void testBindingOfJavaConstructor() throws NoSuchMethodException {
		Constructor<Application> constructor = Application.class.getConstructor(MyInterface.class);

		ModuleBuilder moduleBuilder = ModuleBuilder.create()
			.bind(MyInterface.class).to(MyInterfaceImpl.class)
			.bind(MyInterfaceImpl.class).to(MyInterfaceImpl::new)
			.bind(Application.class).to(constructor);

		Module module = moduleBuilder.build();

		Injector injector = Injector.of(module);

		Application instance = injector.getInstance(Application.class);

		assertNotNull(instance);
		assertThat(instance.myInterface, instanceOf(MyInterfaceImpl.class));
	}

	public interface MyInterface {

	}

	public static class MyInterfaceImpl implements MyInterface {

	}

	public static class Application {
		private final MyInterface myInterface;

		public Application(MyInterface myInterface) {
			this.myInterface = myInterface;
		}
	}
}
