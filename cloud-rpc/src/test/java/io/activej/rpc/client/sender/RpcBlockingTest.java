package io.activej.rpc.client.sender;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

public final class RpcBlockingTest {
	private static final int TIMEOUT = 1500;

	private Thread thread;

	private int port1;
	private int port2;
	private int port3;

	private RpcServer serverOne;
	private RpcServer serverTwo;
	private RpcServer serverThree;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Before
	public void setUp() throws Exception {
		port1 = getFreePort();
		port2 = getFreePort();
		port3 = getFreePort();

		Eventloop eventloop = Reactor.getCurrentReactor();

		serverOne = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class,
						helloServiceRequestHandler(new HelloServiceImplOne()))
				.withListenPort(port1);
		serverOne.listen();

		serverTwo = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class,
						helloServiceRequestHandler(new HelloServiceImplTwo()))
				.withListenPort(port2);
		serverTwo.listen();

		serverThree = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class,
						helloServiceRequestHandler(new HelloServiceImplThree()))
				.withListenPort(port3);
		serverThree.listen();

		thread = new Thread(eventloop);
		thread.start();
	}

	@After
	public void tearDown() throws InterruptedException {
		thread.join();
	}

	@Test
	public void testBlockingCall() throws Exception {
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port1);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port2);
		InetSocketAddress address3 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port3);

		RpcClient client = RpcClient.create(Reactor.getCurrentReactor())
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withStrategy(
						RpcStrategies.RoundRobin.create(
								server(address1),
								RpcStrategies.Sharding.create((HelloRequest item) -> {
											int shard = 0;
											if (item.name.startsWith("S")) {
												shard = 1;
											}
											return shard;
										}, server(address2), server(address3))
										.withMinActiveSubStrategies(2)));

		client.startFuture().get();

		String currentName;
		String currentResponse;

		currentName = "John";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello, " + currentName + "!", currentResponse);

		currentName = "Winston";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello Hello, " + currentName + "!", currentResponse);

		currentName = "Ann";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello, " + currentName + "!", currentResponse);

		currentName = "Emma";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello Hello, " + currentName + "!", currentResponse);

		currentName = "Lukas";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello, " + currentName + "!", currentResponse);

		currentName = "Sophia"; // name starts with "s", so hash code is different from previous examples
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello Hello Hello, " + currentName + "!", currentResponse);

		client.stopFuture().get();

		serverOne.closeFuture().get();
		serverTwo.closeFuture().get();
		serverThree.closeFuture().get();
	}

	private static String blockingRequest(RpcClient rpcClient, String name) throws Exception {
		try {
			return rpcClient.getReactor().submit(
							() -> rpcClient
									.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT))
					.get()
					.message;
		} catch (ExecutionException e) {
			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
			throw (Exception) e.getCause();
		}
	}

	private interface HelloService {
		String hello(String name) throws Exception;
	}

	private static class HelloServiceImplOne implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello, " + name + "!";
		}
	}

	private static class HelloServiceImplTwo implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello Hello, " + name + "!";
		}
	}

	private static class HelloServiceImplThree implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello Hello Hello, " + name + "!";
		}
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
				return Promise.ofException(e);
			}
			return Promise.of(new HelloResponse(result));
		};
	}

}

