package io.activej.datastream.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.MemSize;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.AsyncMessaging;
import io.activej.csp.net.ReactiveMessaging;
import io.activej.datastream.StreamSupplier;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.ReactiveTcpSocket;
import io.activej.promise.Promise;
import io.activej.test.TestUtils;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

import static io.activej.csp.binary.ByteBufsDecoder.ofNullTerminatedBytes;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public final class MessagingWithBinaryStreamingTest {
	private static final ByteBufsCodec<String, String> STRING_SERIALIZER = ByteBufsCodec
			.ofDelimiter(
					ofNullTerminatedBytes(),
					buf -> {
						ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
						buf1.put((byte) 0);
						return buf1;
					})
			.andThen(
					buf -> buf.asString(UTF_8),
					str -> ByteBuf.wrapForReading(str.getBytes(UTF_8)));
	private static final ByteBufsCodec<Integer, Integer> INTEGER_SERIALIZER = STRING_SERIALIZER.andThen(Integer::parseInt, n -> Integer.toString(n));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private int listenPort;
	private InetSocketAddress address;

	private static void pong(AsyncMessaging<Integer, Integer> messaging) {
		messaging.receive()
				.thenIfElse(Objects::nonNull,
						msg -> messaging.send(msg).whenResult(() -> pong(messaging)),
						$ -> {
							messaging.close();
							return Promise.complete();
						})
				.whenException(e -> messaging.close());
	}

	private static void ping(int n, AsyncMessaging<Integer, Integer> messaging) {
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
	public void testPing() throws Exception {
		SimpleServer.create(socket ->
						pong(ReactiveMessaging.create(socket, INTEGER_SERIALIZER)))
				.withListenPort(listenPort)
				.withAcceptOnce()
				.listen();

		await(ReactiveTcpSocket.connect(getCurrentReactor(), address)
				.whenComplete(TestUtils.assertCompleteFn(socket -> ping(3, ReactiveMessaging.create(socket, INTEGER_SERIALIZER)))));
	}

	@Test
	public void testMessagingDownload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.create(
						socket -> {
							ReactiveMessaging<String, String> messaging =
									ReactiveMessaging.create(socket, STRING_SERIALIZER);

							messaging.receive()
									.whenResult(msg -> {
										assertEquals("start", msg);
										StreamSupplier.ofIterable(source)
												.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
														.withInitialBufferSize(MemSize.of(1)))
												.streamTo(messaging.sendBinaryStream());
									});
						})
				.withListenPort(listenPort)
				.withAcceptOnce()
				.listen();

		List<Long> list = await(ReactiveTcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					ReactiveMessaging<String, String> messaging =
							ReactiveMessaging.create(socket, STRING_SERIALIZER);

					return messaging.send("start")
							.then(messaging::sendEndOfStream)
							.then(() -> messaging.receiveBinaryStream()
									.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
									.toList());
				}));

		assertEquals(source, list);
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufsCodec<String, String> serializer = STRING_SERIALIZER;

		SimpleServer.create(
						socket -> {
							ReactiveMessaging<String, String> messaging =
									ReactiveMessaging.create(socket, serializer);

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
				.listen();

		await(ReactiveTcpSocket.connect(getCurrentReactor(), address)
				.whenResult(socket -> {
					ReactiveMessaging<String, String> messaging =
							ReactiveMessaging.create(socket, serializer);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				}));
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufsCodec<String, String> serializer = STRING_SERIALIZER;

		SimpleServer.create(
						socket -> {
							ReactiveMessaging<String, String> messaging = ReactiveMessaging.create(socket, serializer);

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
				.listen();

		String msg = await(ReactiveTcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					ReactiveMessaging<String, String> messaging =
							ReactiveMessaging.create(socket, serializer);

					return messaging.send("start")
							.then(() -> StreamSupplier.ofIterable(source)
									.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
											.withInitialBufferSize(MemSize.of(1)))
									.streamTo(messaging.sendBinaryStream()))
							.then(messaging::receive)
							.whenComplete(messaging::close);
				}));

		assertEquals("ack", msg);
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.create(
						socket -> {
							ReactiveMessaging<String, String> messaging =
									ReactiveMessaging.create(socket, STRING_SERIALIZER);

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
				.listen();

		await(ReactiveTcpSocket.connect(getCurrentReactor(), address)
				.whenResult(socket -> {
					ReactiveMessaging<String, String> messaging =
							ReactiveMessaging.create(socket, STRING_SERIALIZER);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				}));
	}

}
