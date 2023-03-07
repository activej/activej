package io.activej.rpc;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcException;
import io.activej.rpc.protocol.RpcMessageSerializer;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.promise.TestUtils.await;
import static io.activej.rpc.client.sender.strategy.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

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

	@SerializeRecord
	public record HelloRequest(String name) {}

	@SerializeRecord
	public record HelloResponse(String message) {}

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

	private RpcServer createServer(NioReactor reactor) {
		return RpcServer.builder(reactor)
				.withSerializer(RpcMessageSerializer.of(HelloRequest.class, HelloResponse.class))
				.withHandler(HelloRequest.class, helloServiceRequestHandler(name -> {
					if (name.equals("--")) {
						throw new Exception("Illegal name");
					}
					return "Hello, " + name + "!";
				}))
				.withListenPort(port)
				.build();
	}

	private static final int TIMEOUT = 1500;

	private int port;

	@Before
	public void setUp() {
		port = getFreePort();
	}

	@Test
	public void testRpcClientStopBug1() throws Exception {
		doTest(true);
	}

	@Test
	public void testRpcClientStopBug2() throws Exception {
		doTest(false);
	}

	private void doTest(boolean startServerAfterConnectTimeout) throws UnknownHostException, InterruptedException {
		Eventloop eventloopServer = Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.build();
		RpcServer server = createServer(eventloopServer);
		eventloopServer.submit(() -> {
			try {
				server.listen();
			} catch (IOException ignore) {
			}
		});
		Thread serverThread = new Thread(eventloopServer);
		if (!startServerAfterConnectTimeout) {
			serverThread.start();
		}

		NioReactor reactor = Reactor.getCurrentReactor();

		RpcClient rpcClient = RpcClient.builder(reactor)
				.withSerializer(RpcMessageSerializer.of(HelloRequest.class, HelloResponse.class))
				.withStrategy(server(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port)))
				.withConnectTimeout(Duration.ofMillis(TIMEOUT))
				.build();

		try {
			await(rpcClient.start()
					.async()
					.whenComplete(($, e) -> {
						if (e != null) {
							System.err.println(e.getMessage());
							assertThat(e, instanceOf(RpcException.class));
						}
					})
					.then(($1, $2) -> rpcClient.stop())
					.whenComplete(() -> {
						if (startServerAfterConnectTimeout) {
							serverThread.start();
						}
					})
					.whenComplete(() -> System.err.println("Eventloop: " + reactor))
			);
		} finally {
			eventloopServer.submit(server::close);
			serverThread.join();
		}
	}
}
