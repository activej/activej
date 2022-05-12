import io.activej.inject.Injector;
import io.activej.inject.InstanceProvider;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import java.util.Random;

public class InstanceProviderExample {

	public static void main(String[] args) {
		Random random = new Random(System.currentTimeMillis());
		//[START REGION_1]
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bindInstanceProvider(Integer.class);
			}

			@Provides
			Integer giveMe() {
				return random.nextInt(1000);
			}
		};
		//[END REGION_1]

		//[START REGION_2]
		Injector injector = Injector.of(cookbook);
		InstanceProvider<Integer> provider = injector.getInstance(new Key<>() {});
		// lazy value get.
		Integer someInt = provider.get();
		System.out.println(someInt);
		//[END REGION_2]
	}
}
