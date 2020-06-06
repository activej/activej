import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.AbstractModule;

import java.util.Random;

public class TransientBindingExample {

	public static void main(String[] args) {
		Random random = new Random(System.currentTimeMillis());
		//[START REGION_1]
		AbstractModule cookbook = new AbstractModule() {
			@Provides
			@Transient
			Integer giveMe() {
				return random.nextInt(1000);
			}
		};
		//[END REGION_1]

		//[START REGION_2]
		Injector injector = Injector.of(cookbook);
		Integer someInt = injector.getInstance(Integer.class);
		Integer otherInt = injector.getInstance(Integer.class);
		System.out.println("First : " + someInt + ", second  : " + otherInt);
		//[END REGION_2]
	}
}
