package io.activej.launchers.rpc;

import io.activej.inject.annotation.Provides;
import io.activej.rpc.server.RpcServer;
import org.junit.Test;

public class RpcServerLauncherTest {
	@Test
	public void testsInjector() {
		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Provides
			RpcServer rpcServerInitializer() {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}
}
