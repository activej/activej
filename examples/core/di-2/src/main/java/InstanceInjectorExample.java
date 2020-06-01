import io.activej.di.Injector;
import io.activej.di.InstanceInjector;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Provides;
import io.activej.launcher.Launcher;

public final class InstanceInjectorExample extends Launcher {
	//[START REGION_1]
	@Inject
	String message;

	@Provides
	String message() {
		return "Hello, world!";
	}

	@Override
	protected void run() {
		System.out.println(message);
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new InstanceInjectorExample();
		launcher.launch(args);
	}
	//[END REGION_1]

	// internal job of post-creation objects inject.
	//[START REGION_2]
	private void postInjectInstances(String[] args) {
		Injector injector = this.createInjector(args);
		InstanceInjector<Launcher> instanceInjector = injector.getInstanceInjector(Launcher.class);
		instanceInjector.injectInto(this);
	}
	//[END REGION_2]

}
