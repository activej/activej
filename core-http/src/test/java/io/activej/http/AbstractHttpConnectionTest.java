package io.activej.http;

import io.activej.async.function.AsyncFunctionEx;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.MemSize;
import io.activej.common.ref.Ref;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.process.transformer.ChannelTransformers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.http.HttpClient.JmxInspector;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.TestUtils;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpHeaders.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class AbstractHttpConnectionTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private static final byte[] HELLO_WORLD = encodeAscii("Hello, World!");
	private static final Random RANDOM = ThreadLocalRandom.current();

	private HttpClient client;

	private int port;
	private String url;

	@Before
	public void setUp() {
		port = getFreePort();
		url = "http://127.0.0.1:" + port;
		client = HttpClient.builder(Reactor.getCurrentReactor())
				.withInspector(new JmxInspector())
				.build();
	}

	@Test
	public void testMultiLineHeader() throws Exception {
		HttpServer.builder(Reactor.getCurrentReactor(),
						request -> HttpResponse.Builder.ok200()
								.withHeader(DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
								.withHeader(CONTENT_TYPE, "text/\n          html")
								.withBody(wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"))
								.build())
				.withListenPort(port)
				.withAcceptOnce()
				.build()
				.listen();

		await(client.request(HttpRequest.get(url))
				.then(response -> response.loadBody()
						.whenComplete(TestUtils.assertCompleteFn(body -> {
							assertEquals("text/           html", response.getHeader(CONTENT_TYPE));
							assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", body.getString(UTF_8));
						}))));
	}

	@Test
	public void testGzipCompression() throws Exception {
		HttpServer.builder(Reactor.getCurrentReactor(),
						request -> HttpResponse.Builder.ok200()
								.withBodyGzipCompression()
								.withBody(encodeAscii("Test message"))
								.build())
				.withListenPort(port)
				.withAcceptOnce()
				.build()
				.listen();

		await(client.request(HttpRequest.Builder.get(url)
						.withHeader(ACCEPT_ENCODING, "gzip")
						.build())
				.then(response -> response.loadBody()
						.whenComplete(TestUtils.assertCompleteFn(body -> {
							assertEquals("Test message", body.getString(UTF_8));
							assertNotNull(response.getHeader(CONTENT_ENCODING));
						}))));
	}

	@Test
	public void testClientWithMaxKeepAliveRequests() throws Exception {
		client = HttpClient.builder(Reactor.getCurrentReactor())
				.withInspector(new JmxInspector())
				.withKeepAliveTimeout(Duration.ofSeconds(1))
				.withMaxKeepAliveRequests(5)
				.build();

		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.build();
		server.listen();

		assertNotNull(client.getStats());
		checkMaxKeepAlive(5, server, client.getStats().getConnected());
	}

	@Test
	public void testServerWithMaxKeepAliveRequests() throws Exception {
		client = HttpClient.builder(Reactor.getCurrentReactor())
				.withKeepAliveTimeout(Duration.ofSeconds(1))
				.build();

		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.withMaxKeepAliveRequests(5)
				.build();
		server.listen();

		assertNotNull(server.getAccepts());
		checkMaxKeepAlive(5, server, server.getAccepts());
	}

	@Test
	public void testServerWithNoKeepAlive() throws Exception {
		client = HttpClient.builder(Reactor.getCurrentReactor())
				.withKeepAliveTimeout(Duration.ofSeconds(30))
				.build();

		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.withKeepAliveTimeout(Duration.ZERO)
				.build();
		server.listen();

		int code = await(client.request(HttpRequest.get(url))
				.then(response -> {
					assertEquals(200, response.getCode());
					return post(null);
				})
				.then(() -> client.request(HttpRequest.get(url)))
				.map(HttpResponse::getCode)
				.whenComplete(server::close));

		assertEquals(200, code);
	}

	@Test
	@Ignore("Takes a long time")
	public void testHugeBodyStreams() throws IOException {
		int size = 10_000;

		SocketSettings socketSettings = SocketSettings.builder()
				.withSendBufferSize(MemSize.of(1))
				.withReceiveBufferSize(MemSize.of(1))
				.withImplReadBufferSize(MemSize.of(1))
				.build();

		IHttpClient client = HttpClient.builder(Reactor.getCurrentReactor())
				.withSocketSettings(socketSettings)
				.build();

		// regular
		doTestHugeStreams(client, socketSettings, size, httpMessage -> httpMessage.withHeader(CONTENT_LENGTH, String.valueOf(size)));

		// chunked
		doTestHugeStreams(client, socketSettings, size, httpMessage -> {});

		// gzipped + chunked
		doTestHugeStreams(client, socketSettings, size, HttpMessage.Builder::withBodyGzipCompression);
	}

	@Test
	@Ignore("Takes a long time")
	public void testContentLengthPastMaxInt() throws IOException {
		IHttpClient client = HttpClient.create(Reactor.getCurrentReactor());

		Checksum inChecksum = new CRC32();
		Checksum outChecksum = new CRC32();

		long size = 3_000_000_000L;

		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(),
						request -> HttpResponse.Builder.ok200()
								.withHeader(CONTENT_LENGTH, String.valueOf(size))
								.withBodyStream(request.takeBodyStream())
								.build())
				.withListenPort(port)
				.build();
		server.listen();

		HttpRequest request = HttpRequest.Builder.post("http://127.0.0.1:" + port)
				.withHeader(CONTENT_LENGTH, String.valueOf(size))
				.withBodyStream(ChannelSuppliers.ofStream(Stream.generate(() -> {
					int length = 1_000_000;
					byte[] temp = new byte[length];
					RANDOM.nextBytes(temp);
					outChecksum.update(temp, 0, length);
					return ByteBuf.wrapForReading(temp);
				}).limit(3_000)))
				.build();

		await(client.request(request)
				.then(response -> response.takeBodyStream()
						.streamTo(ChannelConsumers.ofConsumer(buf -> {
							byte[] bytes = buf.asArray();
							inChecksum.update(bytes, 0, bytes.length);
						})))
				.whenComplete(server::close));

		assertEquals(inChecksum.getValue(), outChecksum.getValue());
	}

	@Test
	public void testGzipHugeBuf() throws IOException {
		int size = 1_000_000;
		ByteBuf expected = ByteBufPool.allocate(size);
		HttpServer.builder(Reactor.getCurrentReactor(),
						request -> {
							byte[] bytes = new byte[size];
							RANDOM.nextBytes(bytes);
							expected.put(bytes);
							ByteBuf value = expected.slice();
							return HttpResponse.Builder.ok200()
									.withBodyGzipCompression()
									.withBodyStream(ChannelSuppliers.ofValue(value))
									.build();
						})
				.withAcceptOnce()
				.withListenPort(port)
				.build()
				.listen();

		ByteBuf result = await(HttpClient.create(Reactor.getCurrentReactor()).request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.takeBodyStream().toCollector(ByteBufs.collector())));
		assertArrayEquals(expected.asArray(), result.asArray());
	}

	@Test
	public void testEmptyRequestResponse() {
		List<Consumer<HttpMessage.Builder<?, ?>>> messageDecorators = List.of(
				builder -> {},
				HttpMessage.Builder::withBodyGzipCompression,
				builder -> builder.withHeader(CONTENT_LENGTH, "0"),
				builder -> builder
						.withBodyGzipCompression()
						.withHeader(CONTENT_LENGTH, "0"),
				builder -> builder.withBody(ByteBuf.empty()),
				builder -> builder
						.withBody(ByteBuf.empty())
						.withBodyGzipCompression(),
				builder -> builder
						.withBody(ByteBuf.empty())
						.withHeader(CONTENT_LENGTH, "0"),
				builder -> builder
						.withBody(ByteBuf.empty())
						.withBodyGzipCompression()
						.withHeader(CONTENT_LENGTH, "0"),
				builder -> builder.withBodyStream(ChannelSuppliers.empty()),
				builder -> builder
						.withBodyStream(ChannelSuppliers.empty())
						.withBodyGzipCompression(),
				builder -> builder
						.withBodyStream(ChannelSuppliers.empty())
						.withHeader(CONTENT_LENGTH, "0"),
				builder -> builder
						.withBodyStream(ChannelSuppliers.empty())
						.withBodyGzipCompression()
						.withHeader(CONTENT_LENGTH, "0")
		);

		doTestEmptyRequestResponsePermutations(messageDecorators);
	}

	@Test
	public void testHugeUrls() throws IOException {
		char[] chars = new char[16 * 1024];
		Arrays.fill(chars, 'a');

		client = HttpClient.builder(Reactor.getCurrentReactor())
				.withKeepAliveTimeout(Duration.ofSeconds(30))
				.build();

		HttpServer.JmxInspector inspector = new HttpServer.JmxInspector();
		HttpServer.builder(Reactor.getCurrentReactor(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.withInspector(inspector)
				.withAcceptOnce()
				.build()
				.listen();

		HttpResponse response = await(client.request(HttpRequest.get(url + '/' + new String(chars))));

		assertEquals(400, response.getCode());

		ExceptionStats malformedHttpExceptions = inspector.getMalformedHttpExceptions();
		assertEquals(1, malformedHttpExceptions.getTotal());
		Throwable lastException = malformedHttpExceptions.getLastException();
		assert lastException != null;
		assertThat(lastException, instanceOf(MalformedHttpException.class));
		assertThat(lastException.getMessage(), containsString("Header line exceeds max header size"));
	}

	@Test
	public void testKeepAliveWithStreamingResponse() throws Exception {
		JmxInspector clientInspector = new JmxInspector();
		HttpServer.JmxInspector serverInspector = new HttpServer.JmxInspector();

		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(),
						request -> {
							ByteBuf value = ByteBuf.wrapForReading(HELLO_WORLD);
							return HttpResponse.Builder.ok200()
									.withBodyStream(ChannelSuppliers.ofValue(value))
									.build();
						})
				.withInspector(serverInspector)
				.withListenPort(port)
				.build();

		server.listen();

		HttpClient client = HttpClient.builder(Reactor.getCurrentReactor())
				.withKeepAliveTimeout(Duration.ofSeconds(10))
				.withInspector(clientInspector)
				.build();

		assertEquals(0, client.getConnectionsKeepAliveCount());
		assertEquals(0, clientInspector.getConnected().getTotalCount());

		assertEquals(0, server.getConnectionsKeepAliveCount());
		assertEquals(0, serverInspector.getTotalConnections().getTotalCount());

		String url = "http://127.0.0.1:" + port;
		await(client.request(HttpRequest.get(url))
				.then(ensureHelloWorldAsyncFn())
				.whenResult(() -> {
					assertEquals(1, client.getConnectionsKeepAliveCount());
					assertEquals(1, clientInspector.getConnected().getTotalCount());

					assertEquals(1, server.getConnectionsKeepAliveCount());
					assertEquals(1, serverInspector.getTotalConnections().getTotalCount());
				})
				.then(() -> client.request(HttpRequest.get(url)))
				.then(ensureHelloWorldAsyncFn())
				.whenResult(() -> {
					assertEquals(1, client.getConnectionsKeepAliveCount());
					assertEquals(1, clientInspector.getConnected().getTotalCount());

					assertEquals(1, server.getConnectionsKeepAliveCount());
					assertEquals(1, serverInspector.getTotalConnections().getTotalCount());
				})
				.whenComplete(server::close));
	}

	@Test
	public void testKeepAliveWithStreamingRequest() throws Exception {
		JmxInspector clientInspector = new JmxInspector();
		HttpServer.JmxInspector serverInspector = new HttpServer.JmxInspector();

		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(),
						request -> request.loadBody()
								.whenComplete(assertCompleteFn(body -> assertEquals(decodeAscii(HELLO_WORLD), body.getString(UTF_8))))
								.map($ -> HttpResponse.Builder.ok200()
										.withBody(ByteBuf.wrapForReading(HELLO_WORLD))
										.build()))
				.withInspector(serverInspector)
				.withListenPort(port)
				.build();

		server.listen();

		HttpClient client = HttpClient.builder(Reactor.getCurrentReactor())
				.withKeepAliveTimeout(Duration.ofSeconds(10))
				.withInspector(clientInspector)
				.build();

		assertEquals(0, client.getConnectionsKeepAliveCount());
		assertEquals(0, clientInspector.getConnected().getTotalCount());

		assertEquals(0, server.getConnectionsKeepAliveCount());
		assertEquals(0, serverInspector.getTotalConnections().getTotalCount());

		String url = "http://127.0.0.1:" + port;
		ByteBuf value1 = ByteBuf.wrapForReading(HELLO_WORLD);
		await(client.request(HttpRequest.Builder.get(url)
						.withBodyStream(ChannelSuppliers.ofValue(value1))
						.build())
				.then(ensureHelloWorldAsyncFn())
				.whenResult(() -> {
					assertEquals(1, client.getConnectionsKeepAliveCount());
					assertEquals(1, clientInspector.getConnected().getTotalCount());

					assertEquals(1, server.getConnectionsKeepAliveCount());
					assertEquals(1, serverInspector.getTotalConnections().getTotalCount());
				})
				.then(() -> {
					ByteBuf value = ByteBuf.wrapForReading(HELLO_WORLD);
					return client.request(HttpRequest.Builder.get(url)
							.withBodyStream(ChannelSuppliers.ofValue(value))
							.build());
				})
				.then(ensureHelloWorldAsyncFn())
				.whenResult(() -> {
					assertEquals(1, client.getConnectionsKeepAliveCount());
					assertEquals(1, clientInspector.getConnected().getTotalCount());

					assertEquals(1, server.getConnectionsKeepAliveCount());
					assertEquals(1, serverInspector.getTotalConnections().getTotalCount());
				})
				.whenComplete(server::close));
	}

	@Test
	public void testFatalErrorHandling() throws Exception {
		RuntimeException fatalError = new RuntimeException("test");
		Ref<Throwable> errorRef = new Ref<>();

		NioReactor reactor = Eventloop.builder()
				.withCurrentThread()
				.withFatalErrorHandler((e, context) -> {
					assertNull(errorRef.get());
					errorRef.set(e);
				})
				.build();

		HttpServer server = HttpServer.builder(reactor,
						request -> {
							throw fatalError;
						})
				.withListenPort(port)
				.build();

		server.listen();

		IHttpClient client = HttpClient.builder(reactor)
				.withKeepAliveTimeout(Duration.ofSeconds(10))
				.build();

		int responseCode = await(client.request(HttpRequest.get("http://127.0.0.1:" + port))
				.map(HttpResponse::getCode)
				.whenComplete(server::close));

		assertEquals(500, responseCode);
		assertSame(fatalError, errorRef.get());
	}

	private AsyncFunctionEx<HttpResponse, ByteBuf> ensureHelloWorldAsyncFn() {
		return response -> response.loadBody()
				.whenComplete(assertCompleteFn(body -> assertEquals(decodeAscii(HELLO_WORLD), body.getString(UTF_8))))
				.then(AbstractHttpConnectionTest::post);
	}

	private void doTestEmptyRequestResponsePermutations(List<Consumer<HttpMessage.Builder<?, ?>>> messageDecorators) {
		NioReactor reactor = Reactor.getCurrentReactor();
		for (int i = 0; i < messageDecorators.size(); i++) {
			for (int j = 0; j < messageDecorators.size(); j++) {
				try {
					Consumer<HttpMessage.Builder<?, ?>> responseDecorator = messageDecorators.get(j);
					HttpServer server = HttpServer.builder(reactor,
									$ -> {
										HttpResponse.Builder responseBuilder = HttpResponse.Builder.ok200();
										responseDecorator.accept(responseBuilder);
										return responseBuilder.build();
									})
							.withListenPort(port)
							.withAcceptOnce()
							.build();

					try {
						server.listen();
					} catch (IOException e) {
						throw new AssertionError(e);
					}

					HttpRequest.Builder requestBuilder = HttpRequest.Builder.post(url);
					messageDecorators.get(i).accept(requestBuilder);

					String responseText = await(client.request(requestBuilder.build())
							.then(response -> response.loadBody())
							.map(buf -> buf.getString(UTF_8)));

					assertTrue(responseText.isEmpty());
				} catch (AssertionError e) {
					System.out.println("Error while testing request decorator #" + i + " with response decorator #" + j);
					throw e;
				}
				resetPort();
			}
		}
	}

	private void doTestHugeStreams(IHttpClient client, SocketSettings socketSettings, int size, Consumer<HttpMessage.Builder<?, ?>> decorator) throws IOException {
		ByteBuf expected = ByteBufPool.allocate(size);
		HttpServer server = HttpServer.builder(Reactor.getCurrentReactor(),
						request -> request.loadBody()
								.map(body -> {
									HttpResponse.Builder httpResponseBuilder = HttpResponse.Builder.ok200()
											.withBodyStream(request.takeBodyStream()
													.transformWith(ChannelTransformers.chunkBytes(MemSize.of(1), MemSize.of(1))));
									decorator.accept(httpResponseBuilder);
									return httpResponseBuilder.build();
								}))
				.withListenPort(port)
				.withSocketSettings(socketSettings)
				.build();
		server.listen();

		HttpRequest.Builder requestBuilder = HttpRequest.Builder.post("http://127.0.0.1:" + port)
				.withBodyStream(ChannelSuppliers.ofStream(Stream.generate(() -> {
					byte[] temp = new byte[1];
					RANDOM.nextBytes(temp);
					ByteBuf buf = ByteBuf.wrapForReading(temp);
					expected.put(buf.slice());
					return buf;
				}).limit(size)));

		decorator.accept(requestBuilder);

		ByteBuf result = await(client.request(requestBuilder.build())
				.then(response -> response.takeBodyStream()
						.toCollector(ByteBufs.collector()))
				.whenComplete(server::close));
		assertArrayEquals(expected.asArray(), result.asArray());
	}

	private Promise<HttpResponse> checkRequest(String expectedHeader, int expectedConnectionCount, EventStats connectionCount) {
		return client.request(HttpRequest.get(url))
				.then((response, e) -> {
					if (e != null) throw new AssertionError(e);
					assertEquals(expectedHeader, response.getHeader(CONNECTION));
					connectionCount.refresh(System.currentTimeMillis());
					assertEquals(expectedConnectionCount, connectionCount.getTotalCount());
					return Promise.of(response);
				});
	}

	@SuppressWarnings("SameParameterValue")
	private void checkMaxKeepAlive(int maxKeepAlive, HttpServer server, EventStats connectionCount) {
		await(Promises.sequence(
						IntStream.range(0, maxKeepAlive - 1)
								.mapToObj($ ->
										() -> checkRequest("keep-alive", 1, connectionCount)
												.then(AbstractHttpConnectionTest::post)
												.toVoid()))
				.then(() -> checkRequest("close", 1, connectionCount))
				.then(AbstractHttpConnectionTest::post)
				.then(() -> checkRequest("keep-alive", 2, connectionCount))
				.then(AbstractHttpConnectionTest::post)
				.whenComplete(server::close));
	}

	private static <T> Promise<T> post(T value) {
		SettablePromise<T> cb = new SettablePromise<>();
		Reactor.getCurrentReactor().post(() -> cb.set(value));
		return cb;
	}

	private void resetPort() {
		port = getFreePort();
		url = "http://127.0.0.1:" + port;
	}
}
