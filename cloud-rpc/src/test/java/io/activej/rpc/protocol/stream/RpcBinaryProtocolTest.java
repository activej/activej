package io.activej.rpc.protocol.stream;

import io.activej.common.MemSize;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class RpcBinaryProtocolTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private int listenPort;

	@Before
	public void setUp() {
		listenPort = getFreePort();
	}

	@Test
	public void test() throws Exception {
		String testMessage = "Test";

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", listenPort)));

		RpcServer server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withHandler(String.class, request -> Promise.of("Hello, " + request + "!"))
				.withListenPort(listenPort);
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
		BinarySerializer<RpcMessage> binarySerializer = SerializerBuilder.create()
				.withSubclasses(RpcMessage.MESSAGE_TYPES, singletonList(String.class))
				.build(RpcMessage.class);

		int countRequests = 10;

		String testMessage = "Test";
		List<RpcMessage> sourceList = IntStream.range(0, countRequests).mapToObj(i -> RpcMessage.of(i, testMessage)).collect(toList());

		FrameFormat frameFormat = LZ4FrameFormat.create();
		StreamSupplier<RpcMessage> supplier = StreamSupplier.ofIterable(sourceList)
				.transformWith(ChannelSerializer.create(binarySerializer)
						.withInitialBufferSize(MemSize.of(1)))
				.transformWith(ChannelFrameEncoder.create(frameFormat))
				.transformWith(ChannelFrameDecoder.create(frameFormat))
				.transformWith(ChannelDeserializer.create(binarySerializer));

		List<RpcMessage> list = await(supplier.toList());
		assertEquals(countRequests, list.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals(i, list.get(i).getCookie());
			String data = (String) list.get(i).getData();
			assertEquals(testMessage, data);
		}
	}
}
