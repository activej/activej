package io.activej.rpc;

import io.activej.common.time.Stopwatch;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcRemoteException;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class RpcHelloWorldTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

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

	private static RpcServer createServer(Eventloop eventloop) {
		return RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class, helloServiceRequestHandler(name -> {
					if (name.equals("--")) {
						throw new Exception("Illegal name");
					}
					return "Hello, " + name + "!";
				}))
				.withListenPort(port);
	}

	private static class BlockingHelloClient implements HelloService, AutoCloseable {
		private final Eventloop eventloop;
		private final RpcClient rpcClient;

		public BlockingHelloClient(Eventloop eventloop) throws Exception {
			this.eventloop = eventloop;
			this.rpcClient = RpcClient.create(eventloop)
					.withMessageTypes(HelloRequest.class, HelloResponse.class)
					.withStrategy(server(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port)));

			rpcClient.startFuture().get();
		}

		@Override
		public String hello(String name) throws Exception {
			try {
				return rpcClient.getEventloop().submit(
						() -> rpcClient
								.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT))
						.get()
						.message;
			} catch (ExecutionException e) {
				//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
				throw (Exception) e.getCause();
			}
		}

		@Override
		public void close() throws Exception {
			rpcClient.stopFuture().get();
		}
	}

	private static final int TIMEOUT = 1500;
	private static int port;
	private RpcServer server;

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		port = getFreePort();
		server = createServer(eventloop);
		server.listen();
		new Thread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			for (int i = 0; i < 100; i++) {
				assertEquals("Hello, World!", client.hello("World"));
			}
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testAsyncCall() throws Exception {
		int requestCount = 10;

		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			CountDownLatch latch = new CountDownLatch(requestCount);
			for (int i = 0; i < requestCount; i++) {
				String name = "World" + i;
				client.eventloop.execute(() -> client.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
						.whenComplete(latch::countDown)
						.whenComplete(assertCompleteFn(response -> assertEquals("Hello, " + name + "!", response.message))));
			}
			latch.await();
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testBlocking2Clients() throws Exception {
		try (BlockingHelloClient client1 = new BlockingHelloClient(Eventloop.getCurrentEventloop());
			 BlockingHelloClient client2 = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			assertEquals("Hello, John!", client2.hello("John"));
			assertEquals("Hello, World!", client1.hello("World"));
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testBlockingRpcException() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			client.hello("--");
			fail("Exception expected");
		} catch (RpcRemoteException e) {
			assertEquals("java.lang.Exception: Illegal name", e.getMessage());
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testAsync2Clients() throws Exception {
		int requestCount = 10;

		try (BlockingHelloClient client1 = new BlockingHelloClient(Eventloop.getCurrentEventloop());
			 BlockingHelloClient client2 = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			CountDownLatch latch = new CountDownLatch(2 * requestCount);

			for (int i = 0; i < requestCount; i++) {
				String name = "world" + i;
				client1.eventloop.execute(() ->
						client1.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
								.whenComplete(latch::countDown)
								.whenComplete(assertCompleteFn(response -> assertEquals("Hello, " + name + "!", response.message))));
				client2.eventloop.execute(() ->
						client2.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
								.whenComplete(latch::countDown)
								.whenComplete(assertCompleteFn(response -> assertEquals("Hello, " + name + "!", response.message))));
			}
			latch.await();
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	@Ignore("this is not a test but a benchmark, takes a lot of time")
	public void testRejectedRequests() throws Exception {
		int count = 1_000_000;

		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			for (int t = 0; t < 10; t++) {
				AtomicInteger success = new AtomicInteger(0);
				AtomicInteger error = new AtomicInteger(0);
				CountDownLatch latch = new CountDownLatch(count);
				Stopwatch stopwatch = Stopwatch.createStarted();
				for (int i = 0; i < count; i++) {
					client.eventloop.execute(() ->
							client.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest("benchmark"), TIMEOUT)
									.whenComplete(($, e) -> {
										latch.countDown();
										(e == null ? success : error).incrementAndGet();
									}));
				}
				latch.await();
				System.out.printf("%2d: Elapsed %8s rps: %18s (%d/%d [%d])%n",
						t + 1, stopwatch.stop(), count * 1000000.0 / stopwatch.elapsed(MICROSECONDS), success.get(), count, error.get());
			}
		} finally {
			server.closeFuture().get();
		}
	}
}

