package io.activej.rpc;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcException;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.common.Utils.first;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.promise.TestUtils.await;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class RpcNoServerTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private interface HelloService {
		String hello(String name) throws Exception;
	}

	protected static class HelloRequest {
		@Serialize
		public final String name;

		public HelloRequest(@Deserialize("name") String name) {
			this.name = name;
		}
	}

	protected static class HelloResponse {
		@Serialize
		public final String message;

		public HelloResponse(@Deserialize("message") String message) {
			this.message = message;
		}
	}

	private static RpcRequestHandler<HelloRequest, HelloResponse> helloServiceRequestHandler(HelloService helloService) {
		return request -> {
			String result;
			try {
				result = helloService.hello(request.name);
			} catch (Exception e) {
				return Promise.ofException((Exception) e);
			}
			return Promise.of(new HelloResponse(result));
		};
	}

	private RpcServer createServer(Eventloop eventloop) {
		return RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class, helloServiceRequestHandler(name -> {
					if (name.equals("--")) {
						throw new Exception("Illegal name");
					}
					return "Hello, " + name + "!";
				}))
				.withListenPort(0);
	}

	private static final int TIMEOUT = 1500;

	@Test
	public void testRpcClientStopBug1() throws Exception {
		doTest(false);
	}

	@Test
	public void testRpcClientStopBug2() throws Exception {
		doTest(true);
	}

	private void doTest(boolean stopServerAfterConnectTimeout) throws InterruptedException, ExecutionException {
		Eventloop eventloopServer = Eventloop.create().withEventloopFatalErrorHandler(rethrow());
		RpcServer server = createServer(eventloopServer);
		CompletableFuture<Void> listenFuture = eventloopServer.submit(() -> {
			try {
				server.listen();
			} catch (IOException ignore) {
			}
		});
		Thread serverThread = new Thread(eventloopServer);
		serverThread.start();
		listenFuture.get();
		InetSocketAddress address = first(server.getBoundAddresses());
		if (stopServerAfterConnectTimeout) {
			eventloopServer.submit(server::close).get();
		}

		Eventloop eventloopClient = Eventloop.getCurrentEventloop();

		RpcClient rpcClient = RpcClient.create(eventloopClient)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withStrategy(server(address))
				.withConnectTimeout(Duration.ofMillis(TIMEOUT));

		try {
			await(rpcClient.start()
					.async()
					.whenComplete(($, e) -> {
						if (e != null) {
							assertTrue(stopServerAfterConnectTimeout);
							System.err.println(e.getMessage());
							assertThat(e, instanceOf(RpcException.class));
						} else {
							assertFalse(stopServerAfterConnectTimeout);
						}
					})
					.then(($1, $2) -> rpcClient.stop())
					.whenComplete(() -> System.err.println("Eventloop: " + eventloopClient))
			);
		} finally {
			eventloopServer.submit(server::close);
			serverThread.join();
		}
	}
}
