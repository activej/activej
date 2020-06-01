import io.activej.config.Config;
import io.activej.di.Injector;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.di.module.Module;
import io.activej.launcher.Launcher;
import io.activej.launchers.remotefs.RemoteFsServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This example demonstrates configuring and launching RemoteFsServer.
 */
//[START EXAMPLE]
public class ServerSetupExample extends RemoteFsServerLauncher {
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
						.with("remotefs.path", storage.toString())
						.with("remotefs.listenAddresses", "6732");
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
