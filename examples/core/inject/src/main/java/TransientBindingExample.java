import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.AbstractModule;

import java.time.Instant;

public final class TransientBindingExample {

	//[START MODULE]
	static class CurrentTimeModule extends AbstractModule {
		@Provides
		@Transient
		Instant currentInstant() {
			return Instant.now();
		}
	}
	//[END MODULE]

	public static void main(String[] args) throws InterruptedException {
		//[START INJECTOR]
		Injector injector = Injector.of(new CurrentTimeModule());

		System.out.println(injector.getInstance(Instant.class));
		Thread.sleep(1000);
		System.out.println(injector.getInstance(Instant.class));
		//[END INJECTOR]
	}
}
