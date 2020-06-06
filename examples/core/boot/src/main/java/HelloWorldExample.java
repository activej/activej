import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;

//[START EXAMPLE]
public final class HelloWorldExample extends Launcher {
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
		Launcher launcher = new HelloWorldExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
