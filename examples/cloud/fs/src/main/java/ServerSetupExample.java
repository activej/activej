import io.activej.config.Config;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.launchers.fs.ActiveFsServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This example demonstrates configuring and launching ActiveFsServer.
 */
//[START EXAMPLE]
public class ServerSetupExample extends ActiveFsServerLauncher {
	private Path storage;

	@Override
	protected void onInit(Injector injector) throws Exception {
		storage = Files.createTempDirectory("server_storage");
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() {
				return Config.create()
						.with("activefs.path", storage.toString())
						.with("activefs.listenAddresses", "6732");
			}
		};
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new ServerSetupExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
