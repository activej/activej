package io.activej.rpc.protocol.stream;

import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.promise.TestUtils.await;
import static io.activej.rpc.client.RpcClient.DEFAULT_PACKET_SIZE;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.rpc.server.RpcServer.DEFAULT_INITIAL_BUFFER_SIZE;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxMessagesRpcServerTest {
	private static final LZ4FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private int listenPort;
	private RpcServer server;

	@Before
	public void setup() throws IOException {
		listenPort = getFreePort();
		server = RpcServer.create(Reactor.getCurrentReactor())
				.withMessageTypes(String.class)
				.withStreamProtocol(DEFAULT_INITIAL_BUFFER_SIZE, FRAME_FORMAT)
				.withHandler(String.class, request ->
						Promise.of("Hello, " + request + "!"))
				.withListenPort(listenPort)
				.withAcceptOnce();
		server.listen();
	}

	@Test
	public void testWithoutProtocolError() {
		RpcClient client = RpcClient.create(Reactor.getCurrentReactor())
				.withMessageTypes(String.class)
				.withStreamProtocol(DEFAULT_PACKET_SIZE, FRAME_FORMAT)
				.withStrategy(server(new InetSocketAddress("localhost", listenPort)));
		await(client.start().whenResult(() ->
				client.sendRequest("msg", 1000)
						.whenComplete(client::stop)));
		assertEquals(0, server.getFailedRequests().getTotalCount());
	}

	@Test
	public void testWithProtocolError() {
		RpcClient client = RpcClient.create(Reactor.getCurrentReactor())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", listenPort)));
		await(client.start()
				.whenResult(() -> client.sendRequest("msg", 10000)
						.whenComplete(client::stop)));
		assertTrue(server.getLastProtocolError().getTotal() > 0);
	}

	@Test
	public void testWithProtocolError2() {
		RpcClient client = RpcClient.create(Reactor.getCurrentReactor())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", listenPort)));
		await(client.start()
				.whenResult(() -> client.sendRequest("Message larger than LZ4 header", 1000)
						.whenComplete(client::stop)));
		assertTrue(server.getLastProtocolError().getTotal() > 0);
	}
}
