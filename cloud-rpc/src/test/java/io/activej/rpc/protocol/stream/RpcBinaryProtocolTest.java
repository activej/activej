package io.activej.rpc.protocol.stream;

import io.activej.common.MemSize;
import io.activej.csp.process.ChannelLZ4Compressor;
import io.activej.csp.process.ChannelLZ4Decompressor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcRemoteException;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class RpcBinaryProtocolTest {
	private static final int LISTEN_PORT = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws Exception {
		String testMessage = "Test";

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));

		RpcServer server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, request -> Promise.of("Hello, " + request + "!"))
				.withListenPort(LISTEN_PORT);
		server.listen();

		int countRequests = 10;

		List<String> list = await(client.start()
				.then(() ->
						Promises.toList(IntStream.range(0, countRequests)
								.mapToObj(i -> client.<String, String>sendRequest(testMessage, 1000))))
				.whenComplete(() -> {
					client.stop();
					server.close();
				}));

		assertTrue(list.stream().allMatch(response -> response.equals("Hello, " + testMessage + "!")));
	}

	@Test
	public void testCompression() {
		BinarySerializer<RpcMessage> binarySerializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, String.class)
				.build(RpcMessage.class);

		int countRequests = 10;

		String testMessage = "Test";
		List<RpcMessage> sourceList = IntStream.range(0, countRequests).mapToObj(i -> RpcMessage.of(i, testMessage)).collect(toList());

		StreamSupplier<RpcMessage> supplier = StreamSupplier.ofIterable(sourceList)
				.transformWith(ChannelSerializer.create(binarySerializer)
						.withInitialBufferSize(MemSize.of(1))
						.withMaxMessageSize(MemSize.of(64)))
				.transformWith(ChannelLZ4Compressor.createFastCompressor())
				.transformWith(ChannelLZ4Decompressor.create())
				.transformWith(ChannelDeserializer.create(binarySerializer));

		List<RpcMessage> list = await(supplier.toList());
		assertEquals(countRequests, list.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals(i, list.get(i).getCookie());
			String data = (String) list.get(i).getData();
			assertEquals(testMessage, data);
		}
	}

	@Test
	public void testSerializationErrorOnClient() throws IOException {
		String testMessage = "12345";

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStreamProtocol(MemSize.bytes(1), MemSize.bytes(3), false)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));

		RpcServer server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, Promise::of)
				.withListenPort(LISTEN_PORT);
		server.listen();

		ArrayIndexOutOfBoundsException e = awaitException(client.start()
				.then($ -> client.<String, String>sendRequest(testMessage))
				.whenComplete(() -> {
					client.stop();
					server.close();
				}));

		assertEquals(e.getMessage(), "Message overflow");
	}

	@Test
	public void testSerializationErrorOnServer() throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000; i++) {
			sb.append("test");
		}

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));

		RpcServer server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStreamProtocol(MemSize.bytes(100), MemSize.bytes(1000), false)
				.withHandler(String.class, String.class, Promise::of)
				.withListenPort(LISTEN_PORT);
		server.listen();

		RpcRemoteException e = awaitException(client.start()
				.then($ -> client.<String, String>sendRequest(sb.toString()))
				.whenComplete(() -> {
					client.stop();
					server.close();
				}));

		assertEquals("Message overflow", e.getCauseMessage());
		assertEquals(ArrayIndexOutOfBoundsException.class.getName(), e.getCauseClassName());
	}

}
