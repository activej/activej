package io.activej.rpc.protocol.stream;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.promise.TestUtils.await;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxMessagesRpcServerTest {
	private final int LISTEN_PORT = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	RpcServer server;

	@Before
	public void setup() throws IOException {
		server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withHandler(String.class, request ->
						Promise.of("Hello, " + request + "!"))
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce();
		server.listen();
	}

	@Test
	public void testWithoutProtocolError() {
		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));
		await(client.start().whenResult(() ->
				client.sendRequest("msg", 1000)
						.whenComplete(() -> {
							assertEquals(0, server.getFailedRequests().getTotalCount());
							client.stop();
						})));
	}

	@Test
	public void testWithProtocolError() {
		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));
		await(client.start()
				.whenResult(() -> client.sendRequest("msg", 10000)
						.whenComplete(() -> {
							assertTrue(server.getLastProtocolError().getTotal() > 0);
							client.stop();
						})));
	}

	@Test
	public void testWithProtocolError2() {
		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));
		await(client.start()
				.whenResult(() -> client.sendRequest("Message larger than LZ4 header", 1000)
						.whenComplete(() -> {
							assertTrue(server.getLastProtocolError().getTotal() > 0);
							client.stop();
						})));
	}
}
