import io.activej.async.service.ReactiveService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.service.ServiceGraphModule;

import java.time.Duration;

//[START EXAMPLE]
public class ReactiveServiceExample extends Launcher {

	@Provides
	Reactor reactor() {
		return Eventloop.create();
	}

	@Provides
	@Eager
	CustomReactiveService customEventloopService(Reactor reactor) {
		return new CustomReactiveService(reactor);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() {
		System.out.println("|RUNNING|");
	}

	private static final class CustomReactiveService extends AbstractReactive implements ReactiveService {
		public CustomReactiveService(Reactor reactor) {
			super(reactor);
		}

		@Override
		public Promise<?> start() {
			System.out.println("|CUSTOM EVENTLOOP SERVICE STARTING|");
			return Promises.delay(Duration.ofMillis(10))
					.whenResult(() -> System.out.println("|CUSTOM EVENTLOOP SERVICE STARTED|"));
		}

		@Override
		public Promise<?> stop() {
			System.out.println("|CUSTOM EVENTLOOP SERVICE STOPPING|");
			return Promises.delay(Duration.ofMillis(10))
					.whenResult(() -> System.out.println("|CUSTOM EVENTLOOP SERVICE STOPPED|"));
		}
	}

	public static void main(String[] args) throws Exception {
		ReactiveServiceExample example = new ReactiveServiceExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
