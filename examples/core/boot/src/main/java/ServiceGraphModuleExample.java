import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.service.ServiceGraph;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.ExecutionException;

//[START EXAMPLE]
public final class ServiceGraphModuleExample extends AbstractModule {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Injector injector = Injector.of(ServiceGraphModule.create(), new ServiceGraphModuleExample());
		Eventloop eventloop = injector.getInstance(Eventloop.class);

		eventloop.execute(() -> System.out.println("\nHello World\n"));

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
//[END EXAMPLE]
