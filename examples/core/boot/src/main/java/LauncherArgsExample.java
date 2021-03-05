import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Inject;
import io.activej.launcher.Launcher;
import io.activej.launcher.annotation.Args;

import java.util.Arrays;

/**
 * This example demonstrates two ways of obtaining program arguments passed to the application
 * <p>
 * <ul>
 *     <li>Arguments are stored in {@link Launcher#args} field</li>
 *     <li>Arguments can be retrieved using dependency injection.
 *     By querying a key of type {@code String[]} annotated with {@link Args} annotation</li>
 * </ul>
 */
//[START EXAMPLE]
public final class LauncherArgsExample extends Launcher {

	@Inject
	Injector injector;

	@Override
	protected void run() {
		System.out.println("Received args: " + Arrays.toString(args));

		String[] injectedArgs = injector.getInstance(Key.of(String[].class, Args.class));
		System.out.println("Args retrieved from DI: " + Arrays.toString(injectedArgs));
	}

	public static void main(String[] args) throws Exception {
		new LauncherArgsExample().launch(args);
	}
}
//[END EXAMPLE]
