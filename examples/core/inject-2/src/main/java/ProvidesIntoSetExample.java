import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;

import java.util.Set;

public final class ProvidesIntoSetExample {
	public static void main(String[] args) {
		//[START INJECTOR]
		Module module1 = new MyModule1();
		Module module2 = new MyModule2();
		Module module3 = new MyModule3();
		Injector injector = Injector.of(module1, module2, module3);
		System.out.println(injector.getInstance(new Key<Set<Integer>>() {}));
		//[END INJECTOR]
	}

	//[START MODULE_1]
	public static final class MyModule1 extends AbstractModule {
		@ProvidesIntoSet
		Integer integer() {
			return 1;
		}
	}
	//[END MODULE_1]

	//[START MODULE_2]
	public static final class MyModule2 extends AbstractModule {
		@ProvidesIntoSet
		Integer integer() {
			return 2;
		}
	}
	//[END MODULE_2]

	//[START MODULE_3]
	public static final class MyModule3 extends AbstractModule {
		@ProvidesIntoSet
		Integer integer() {
			return 3;
		}
	}
	//[END MODULE_3]
}
