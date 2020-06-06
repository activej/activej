import io.activej.inject.annotation.Inject;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;

import static io.activej.inject.module.Modules.combine;

// [START EXAMPLE]
public class ServerLauncher extends Launcher {
	@Inject
	private RpcServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				new ServerModule());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		ServerLauncher launcher = new ServerLauncher();
		launcher.launch(args);
	}
}
// [END EXAMPLE]
