package io.activej.launchers.rpc;

import io.activej.inject.annotation.Provides;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

public class RpcServerLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

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
