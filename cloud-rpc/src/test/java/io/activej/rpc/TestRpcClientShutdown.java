package io.activej.rpc;

import io.activej.async.exception.AsyncCloseException;
import io.activej.common.ref.RefInt;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.RpcClientConnection;
import io.activej.rpc.server.RpcServer;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.rpc.client.sender.strategy.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

public final class TestRpcClientShutdown {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private int port;

	@Before
	public void setUp() {
		port = getFreePort();
	}

	@Test
	public void testServerOnClientShutdown() throws IOException {
		NioReactor reactor = Reactor.getCurrentReactor();
		Executor executor = Executors.newSingleThreadExecutor();
		List<Class<?>> messageTypes = List.of(Request.class, Response.class);

		RpcServer rpcServer = RpcServer.builder(reactor)
			.withMessageTypes(messageTypes)
			.withHandler(Request.class,
				request -> Promise.ofBlocking(executor, () -> {
					Thread.sleep(100);
					return new Response();
				}))
			.withListenPort(port)
			.build();

		RpcClient rpcClient = RpcClient.builder(reactor)
			.withMessageTypes(messageTypes)
			.withStrategy(server(new InetSocketAddress(port)))
			.build();

		rpcServer.listen();

		Exception exception = awaitException(rpcClient.start()
			.then(() -> Promises.all(
				rpcClient.sendRequest(new Request())
					.whenComplete(() -> {
						for (RpcClientConnection conn : rpcClient.getRequestStatsPerConnection().values()) {
							conn.onSenderError(new ExpectedException());
						}
					}),
				rpcClient.sendRequest(new Request()),
				rpcClient.sendRequest(new Request()),
				rpcClient.sendRequest(new Request()),
				rpcClient.sendRequest(new Request())
			))
			.whenComplete(rpcClient::stop)
			.whenComplete(rpcServer::close)
		);

		assertThat(exception, instanceOf(AsyncCloseException.class));
	}

	@Test
	public void testClientForcedShutdown() throws IOException {
		NioReactor reactor = Reactor.getCurrentReactor();
		List<Class<?>> messageTypes = List.of(Request.class, Response.class);

		RpcServer rpcServer = RpcServer.builder(reactor)
			.withMessageTypes(messageTypes)
			.withHandler(Request.class,
				request -> Promises.delay(Duration.ofSeconds(Integer.MAX_VALUE), new Response()))
			.withListenPort(port)
			.build();

		RpcClient rpcClient = RpcClient.builder(reactor)
			.withForcedShutdown()
			.withMessageTypes(messageTypes)
			.withStrategy(server(new InetSocketAddress(port)))
			.build();

		rpcServer.listen();

		int requests = 10;
		Integer closed = await(rpcClient.start()
			.then(() -> {
				RefInt closedRef = new RefInt(0);
				for (int i = 0; i < requests; i++) {
					rpcClient.sendRequest(new Request())
						.whenException(AsyncCloseException.class, e -> closedRef.inc());
				}
				return rpcClient.stop()
					.map($ -> closedRef.get());
			})
			.whenComplete(rpcServer::close));

		assertEquals(requests, closed.intValue());
	}

	public static final class Request {
	}

	public static final class Response {
	}
}
