package io.activej.launchers.rpc;

import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.rpc.server.RpcServer;
import io.activej.worker.annotation.Worker;
import org.junit.Test;

public class MultithreadedRpcServerLauncherTest {

	@Test
	public void testsInjector() {
		MultithreadedRpcServerLauncher launcher = new MultithreadedRpcServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return new AbstractModule() {
					@Provides
					@Worker
					RpcServer rpcServerInitializer() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
		launcher.testInjector();
	}
}
