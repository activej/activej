import io.activej.async.service.ReactorService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.service.ServiceGraphModule;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

//[START EXAMPLE]
public class ReactorServiceExample extends Launcher {

	@Provides
	Reactor reactor() {
		return Eventloop.create();
	}

	@Provides
	@Eager
	CustomReactorService customEventloopService(Reactor reactor) {
		return new CustomReactorService(reactor);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() {
		System.out.println("|RUNNING|");
	}

	private static final class CustomReactorService implements ReactorService {
		private final Reactor reactor;

		public CustomReactorService(Reactor reactor) {
			this.reactor = reactor;
		}

		@Override
		public @NotNull Reactor getReactor() {
			return reactor;
		}

		@Override
		public @NotNull Promise<?> start() {
			System.out.println("|CUSTOM EVENTLOOP SERVICE STARTING|");
			return Promises.delay(Duration.ofMillis(10))
					.whenResult(() -> System.out.println("|CUSTOM EVENTLOOP SERVICE STARTED|"));
		}

		@Override
		public @NotNull Promise<?> stop() {
			System.out.println("|CUSTOM EVENTLOOP SERVICE STOPPING|");
			return Promises.delay(Duration.ofMillis(10))
					.whenResult(() -> System.out.println("|CUSTOM EVENTLOOP SERVICE STOPPED|"));
		}
	}

	public static void main(String[] args) throws Exception {
		ReactorServiceExample example = new ReactorServiceExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
