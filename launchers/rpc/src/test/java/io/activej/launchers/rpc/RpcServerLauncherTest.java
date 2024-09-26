package io.activej.launchers.rpc;

import io.activej.inject.annotation.Provides;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.BindException;
import java.net.InetSocketAddress;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

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

	@Test(timeout = 5_000L)
	public void testServerWithInvalidAddressProperlyStopsOnLaunch() {
		InetSocketAddress randomPortAddress = new InetSocketAddress(0);
		InetSocketAddress invalidAddress = new InetSocketAddress("1.1.1.1", 0);

		RpcServerLauncher launcher = new RpcServerLauncher() {
			@Provides
			RpcServer server(NioReactor reactor) {
				return RpcServer.builder(reactor)
					.withListenAddresses(randomPortAddress, invalidAddress)
					.withMessageTypes(Integer.class)
					.withHandler(Integer.class, Promise::of)
					.build();
			}
		};

		Exception e = assertThrows(Exception.class, () -> launcher.launch(new String[0]));
		assertThat(e, instanceOf(BindException.class));
	}
}
