package io.activej.launchers.rpc;

import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.ReactiveRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.rpc.client.sender.RpcStrategyRoundRobin;
import io.activej.rpc.protocol.RpcException;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraph;
import io.activej.service.ServiceGraphModule;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RpcServiceGraphTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private int port;

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Reactor.getCurrentReactor();
		port = getFreePort();
		RpcServer server = RpcServer.create(eventloop)
				.withMessageTypes(String.class)
				.withHandler(String.class, Promise::of)
				.withListenPort(port);
		server.listen();
		new Thread(eventloop).start();
	}

	@Test(timeout = 5_000)
	public void testPartialConnectionOnStart() throws InterruptedException {
		Injector injector = Injector.of(
				ServiceGraphModule.create(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.create();
					}

					@Provides
					@Eager
					RpcClient client(Eventloop eventloop) {
						return ReactiveRpcClient.create(eventloop)
								.withMessageTypes(String.class)
								.withStrategy(RpcStrategyRoundRobin.create(
												RpcStrategies.servers(
														new InetSocketAddress(port),
														new InetSocketAddress(getFreePort())
												))
										.withMinActiveSubStrategies(2));
					}
				}
		);

		injector.createEagerInstances();
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} catch (ExecutionException e){
			Throwable cause = e.getCause();
			assertTrue(cause instanceof RpcException);
			assertEquals("Could not establish connection", cause.getMessage());
		}

		try {
			serviceGraph.stopFuture().get();
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
