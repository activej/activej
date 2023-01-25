package io.activej.csp.net;

import io.activej.async.exception.AsyncCloseException;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.exception.TruncatedDataException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.net.socket.tcp.TcpSocket_Ssl;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

public final class SslReactive_TcpSocketTest {
	private static final String KEYSTORE_PATH = "./src/test/resources/keystore.jks";
	private static final String KEYSTORE_PASS = "testtest";
	private static final String KEY_PASS = "testtest";

	private static final String TRUSTSTORE_PATH = "./src/test/resources/truststore.jks";
	private static final String TRUSTSTORE_PASS = "testtest";

	private static final String TEST_STRING = "Hello world";

	private static final ByteBufsDecoder<String> DECODER = ByteBufsDecoder.ofFixedSize(TEST_STRING.length())
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	public static final int LARGE_STRING_SIZE = 10_000;
	public static final int SMALL_STRING_SIZE = 1000;
	private static final int LENGTH = LARGE_STRING_SIZE + TEST_STRING.length() * SMALL_STRING_SIZE;

	private static final ByteBufsDecoder<String> DECODER_LARGE = ByteBufsDecoder.ofFixedSize(LENGTH)
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private NioReactor reactor;
	private Executor executor;
	private SSLContext sslContext;
	private StringBuilder sentData;
	private InetSocketAddress address;

	@Before
	public void setUp() throws Exception {
		reactor = getCurrentReactor();
		executor = Executors.newSingleThreadExecutor();
		sslContext = createSslContext();
		sentData = new StringBuilder();
		address = new InetSocketAddress("localhost", getFreePort());
	}

	@Test
	public void testWrite() throws IOException {
		startServer(sslContext, sslSocket -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
				.decode(DECODER)
				.whenComplete(sslSocket::close)
				.whenComplete(assertCompleteFn(result -> assertEquals(TEST_STRING, result))));

		await(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.then(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING))
								.whenComplete(sslSocket::close)));
	}

	@Test
	public void testRead() throws IOException {
		startServer(sslContext, sslSocket ->
				sslSocket.write(wrapAscii(TEST_STRING))
						.whenComplete(assertCompleteFn()));

		String result = await(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.then(sslSocket -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
						.decode(DECODER)
						.whenComplete(sslSocket::close)));

		assertEquals(TEST_STRING, result);
	}

	@Test
	public void testLoopBack() throws IOException {
		startServer(sslContext, serverSsl -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(serverSsl))
				.decode(DECODER)
				.then(result -> serverSsl.write(wrapAscii(result)))
				.whenComplete(serverSsl::close)
				.whenComplete(assertCompleteFn()));

		String result = await(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.then(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING))
								.then(() -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
										.decode(DECODER))
								.whenComplete(sslSocket::close)));

		assertEquals(TEST_STRING, result);
	}

	@Test
	public void testLoopBackWithEmptyBufs() throws IOException {
		int halfLength = TEST_STRING.length() / 2;
		String TEST_STRING_PART_1 = TEST_STRING.substring(0, halfLength);
		String TEST_STRING_PART_2 = TEST_STRING.substring(halfLength);
		startServer(sslContext, serverSsl -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(serverSsl))
				.decode(DECODER)
				.then(result -> serverSsl.write(wrapAscii(result)))
				.whenComplete(serverSsl::close)
				.whenComplete(assertCompleteFn()));

		String result = await(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.then(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING_PART_1))
								.then(() -> sslSocket.write(ByteBuf.empty()))
								.then(() -> sslSocket.write(wrapAscii(TEST_STRING_PART_2)))
								.then(() -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
										.decode(DECODER))
								.whenComplete(sslSocket::close)));

		assertEquals(TEST_STRING, result);
	}

	@Test
	public void sendsLargeAmountOfDataFromClientToServer() throws IOException {
		startServer(sslContext, serverSsl -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(serverSsl))
				.decode(DECODER_LARGE)
				.whenComplete(serverSsl::close)
				.whenComplete(assertCompleteFn(result -> assertEquals(result, sentData.toString()))));

		await(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.whenResult(sslSocket ->
						sendData(sslSocket)
								.whenComplete(sslSocket::close)));
	}

	@Test
	public void sendsLargeAmountOfDataFromServerToClient() throws IOException {
		startServer(sslContext, serverSsl ->
				sendData(serverSsl)
						.whenComplete(serverSsl::close)
						.whenComplete(assertCompleteFn()));

		String result = await(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.then(sslSocket -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
						.decode(DECODER_LARGE)
						.whenComplete(sslSocket::close)));

		assertEquals(sentData.toString(), result);
	}

	@Test
	public void testCloseAndOperationAfterClose() throws IOException {
		startServer(sslContext, socket ->
				socket.write(wrapAscii("He"))
						.whenComplete(socket::close)
						.then(() -> socket.write(wrapAscii("ello")))
						.whenComplete(($, e) -> assertThat(e, instanceOf(AsyncCloseException.class))));

		Exception e = awaitException(TcpSocket.connect(reactor, address)
				.map(socket -> TcpSocket_Ssl.wrapClientSocket(reactor, socket, sslContext, executor))
				.then(sslSocket -> {
					BinaryChannelSupplier supplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket));
					return supplier.decode(DECODER)
							.whenException(supplier::closeEx);
				}));

		assertThat(e, instanceOf(TruncatedDataException.class));
	}

	@Test
	public void testPeerClosingDuringHandshake() throws IOException {
		ServerSocket listener = new ServerSocket(address.getPort());
		Thread serverThread = new Thread(() -> {
			try (Socket ignored = listener.accept()) {
				listener.close();
			} catch (IOException ignored) {
				throw new AssertionError();
			}
		});

		serverThread.start();

		Exception exception = awaitException(TcpSocket.connect(reactor, address)
				.whenResult(tcpSocket -> {
					try {
						// noinspection ConstantConditions - Imitating a suddenly closed channel
						tcpSocket.getSocketChannel().close();
					} catch (IOException e) {
						throw new AssertionError();
					}
				})
				.map(tcpSocket -> TcpSocket_Ssl.wrapClientSocket(reactor, tcpSocket, sslContext, executor))
				.then(socket -> socket.write(ByteBufStrings.wrapUtf8("hello"))));
		assertThat(exception, instanceOf(AsyncCloseException.class));
	}

	void startServer(SSLContext sslContext, Consumer<AsyncTcpSocket> logic) throws IOException {
		SimpleServer.builder(Reactor.getCurrentReactor(), logic)
				.withSslListenAddress(sslContext, Executors.newSingleThreadExecutor(), address)
				.withAcceptOnce()
				.build()
				.listen();
	}

	static SSLContext createSslContext() throws Exception {
		SSLContext instance = SSLContext.getInstance("TLSv1.2");

		KeyStore keyStore = KeyStore.getInstance("JKS");
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		try (InputStream input = new FileInputStream(KEYSTORE_PATH)) {
			keyStore.load(input, KEYSTORE_PASS.toCharArray());
		}
		kmf.init(keyStore, KEY_PASS.toCharArray());

		KeyStore trustStore = KeyStore.getInstance("JKS");
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		try (InputStream input = new FileInputStream(TRUSTSTORE_PATH)) {
			trustStore.load(input, TRUSTSTORE_PASS.toCharArray());
		}
		tmf.init(trustStore);

		instance.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
		return instance;
	}

	public static String generateLargeString(int size) {
		StringBuilder builder = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < size; i++) {
			int randNumber = random.nextInt(3);
			if (randNumber == 0) {
				builder.append('a');
			} else if (randNumber == 1) {
				builder.append('b');
			} else {
				builder.append('c');
			}
		}
		return builder.toString();
	}

	private Promise<?> sendData(AsyncTcpSocket socket) {
		String largeData = generateLargeString(LARGE_STRING_SIZE);
		ByteBuf largeBuf = wrapAscii(largeData);
		sentData.append(largeData);

		return socket.write(largeBuf)
				.then(() -> Promises.loop(SMALL_STRING_SIZE,
						i -> i != 0,
						i -> {
							sentData.append(TEST_STRING);
							return socket.write(wrapAscii(TEST_STRING))
									.async()
									.map($2 -> i - 1);
						}));
	}
}
