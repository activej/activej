package io.activej.datastream.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.MemSize;
import io.activej.csp.binary.codec.ByteBufsCodec;
import io.activej.csp.binary.codec.ByteBufsCodecs;
import io.activej.csp.net.IMessaging;
import io.activej.csp.net.Messaging;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.TestUtils;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

import static io.activej.csp.binary.decoder.ByteBufsDecoders.ofNullTerminatedBytes;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public final class MessagingTest {
	private static final ByteBufsCodec<String, String> STRING_SERIALIZER = ByteBufsCodecs
			.ofDelimiter(
					ofNullTerminatedBytes(),
					buf -> {
						ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
						buf1.put((byte) 0);
						return buf1;
					})
			.transform(
					buf -> buf.asString(UTF_8),
					str -> ByteBuf.wrapForReading(str.getBytes(UTF_8)));
	private static final ByteBufsCodec<Integer, Integer> INTEGER_SERIALIZER = STRING_SERIALIZER.transform(Integer::parseInt, n -> Integer.toString(n));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private int listenPort;
	private InetSocketAddress address;

	private static void pong(IMessaging<Integer, Integer> messaging) {
		messaging.receive()
				.thenIfElse(Objects::nonNull,
						msg -> messaging.send(msg).whenResult(() -> pong(messaging)),
						$ -> {
							messaging.close();
							return Promise.complete();
						})
				.whenException(e -> messaging.close());
	}

	private static void ping(int n, IMessaging<Integer, Integer> messaging) {
		messaging.send(n)
				.then(messaging::receive)
				.whenResult(msg -> {
					if (msg != null) {
						if (msg > 0) {
							ping(msg - 1, messaging);
						} else {
							messaging.close();
						}
					}
				})
				.whenException(e -> messaging.close());
	}

	@Before
	public void setUp() {
		listenPort = getFreePort();
		address = new InetSocketAddress("localhost", listenPort);
	}

	@Test
	public void testPing() throws IOException {
		SimpleServer.builder(Reactor.getCurrentReactor(), socket ->
						pong(Messaging.create(socket, INTEGER_SERIALIZER)))
				.withListenPort(listenPort)
				.withAcceptOnce()
				.build()
				.listen();

		await(TcpSocket.connect(getCurrentReactor(), address)
				.whenComplete(TestUtils.assertCompleteFn(socket -> ping(3, Messaging.create(socket, INTEGER_SERIALIZER)))));
	}

	@Test
	public void testMessagingDownload() throws IOException {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.builder(
						Reactor.getCurrentReactor(),
						socket -> {
							Messaging<String, String> messaging =
									Messaging.create(socket, STRING_SERIALIZER);

							messaging.receive()
									.whenResult(msg -> {
										assertEquals("start", msg);
										StreamSuppliers.ofIterable(source)
												.transformWith(ChannelSerializer.builder(LONG_SERIALIZER)
														.withInitialBufferSize(MemSize.of(1))
														.build())
												.streamTo(messaging.sendBinaryStream());
									});
						})
				.withListenPort(listenPort)
				.withAcceptOnce()
				.build()
				.listen();

		List<Long> list = await(TcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					Messaging<String, String> messaging =
							Messaging.create(socket, STRING_SERIALIZER);

					return messaging.send("start")
							.then(messaging::sendEndOfStream)
							.then(() -> messaging.receiveBinaryStream()
									.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
									.toList());
				}));

		assertEquals(source, list);
	}

	@Test
	public void testBinaryMessagingUpload() throws IOException {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufsCodec<String, String> serializer = STRING_SERIALIZER;

		SimpleServer.builder(
						Reactor.getCurrentReactor(),
						socket -> {
							Messaging<String, String> messaging =
									Messaging.create(socket, serializer);

							messaging.receive()
									.whenComplete(TestUtils.assertCompleteFn(msg -> assertEquals("start", msg)))
									.then(() ->
											messaging.receiveBinaryStream()
													.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
													.toList()
													.then(list ->
															messaging.sendEndOfStream().map($2 -> list)))
									.whenComplete(TestUtils.assertCompleteFn(list -> assertEquals(source, list)));
						})
				.withListenPort(listenPort)
				.withAcceptOnce()
				.build()
				.listen();

		await(TcpSocket.connect(getCurrentReactor(), address)
				.whenResult(socket -> {
					Messaging<String, String> messaging =
							Messaging.create(socket, serializer);

					messaging.send("start");

					StreamSuppliers.ofIterable(source)
							.transformWith(ChannelSerializer.builder(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1))
									.build())
							.streamTo(messaging.sendBinaryStream());
				}));
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufsCodec<String, String> serializer = STRING_SERIALIZER;

		SimpleServer.builder(
						Reactor.getCurrentReactor(),
						socket -> {
							Messaging<String, String> messaging = Messaging.create(socket, serializer);

							messaging.receive()
									.whenResult(msg -> assertEquals("start", msg))
									.then(msg ->
											messaging.receiveBinaryStream()
													.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
													.toList()
													.then(list ->
															messaging.send("ack")
																	.then(messaging::sendEndOfStream)
																	.map($ -> list)))
									.whenComplete(TestUtils.assertCompleteFn(list -> assertEquals(source, list)));
						})
				.withListenPort(listenPort)
				.withAcceptOnce()
				.build()
				.listen();

		String msg = await(TcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					Messaging<String, String> messaging =
							Messaging.create(socket, serializer);

					return messaging.send("start")
							.then(() -> StreamSuppliers.ofIterable(source)
									.transformWith(ChannelSerializer.builder(LONG_SERIALIZER)
											.withInitialBufferSize(MemSize.of(1))
											.build())
									.streamTo(messaging.sendBinaryStream()))
							.then(messaging::receive)
							.whenComplete(messaging::close);
				}));

		assertEquals("ack", msg);
	}

	@Test
	public void testGsonMessagingUpload() throws IOException {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.builder(
						Reactor.getCurrentReactor(),
						socket -> {
							Messaging<String, String> messaging =
									Messaging.create(socket, STRING_SERIALIZER);

							messaging.receive()
									.whenComplete(TestUtils.assertCompleteFn(msg -> assertEquals("start", msg)))
									.then(msg -> messaging.sendEndOfStream())
									.then(msg ->
											messaging.receiveBinaryStream()
													.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
													.toList())
									.whenComplete(TestUtils.assertCompleteFn(list -> assertEquals(source, list)));
						})
				.withListenPort(listenPort)
				.withAcceptOnce()
				.build()
				.listen();

		await(TcpSocket.connect(getCurrentReactor(), address)
				.whenResult(socket -> {
					Messaging<String, String> messaging =
							Messaging.create(socket, STRING_SERIALIZER);

					messaging.send("start");

					StreamSuppliers.ofIterable(source)
							.transformWith(ChannelSerializer.builder(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1))
									.build())
							.streamTo(messaging.sendBinaryStream());
				}));
	}

}
