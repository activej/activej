import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;

import static io.activej.codegen.expression.Expression.*;

public final class CodegenExpressionsExample {
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	public static void main(String[] args) throws ReflectiveOperationException {
		//[START REGION_1]
		Class<Greeter> greeterClass = ClassBuilder.builder(Greeter.class)
				.withMethod("sayHello",
						call(staticField(System.class, "out"), "println", value("Hello world")))
				.build()
				.defineClass(CLASS_LOADER);
		//[END REGION_1]

		//[START REGION_2]
		Greeter greeter = greeterClass.getDeclaredConstructor().newInstance();
		greeter.sayHello();
		//[END REGION_2]
	}

	//[START REGION_3]
	public interface Greeter {
		void sayHello();
	}
	//[END REGION_3]
}
