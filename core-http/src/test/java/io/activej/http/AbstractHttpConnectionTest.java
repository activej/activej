package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.MemSize;
import io.activej.common.function.FunctionEx;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.SocketSettings;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.test.TestUtils;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
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
import static java.util.Arrays.asList;
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

	private AsyncHttpClient client;

	private int port;
	private String url;

	@Before
	public void setUp() {
		port = getFreePort();
		url = "http://127.0.0.1:" + port;
		client = AsyncHttpClient.create(Eventloop.getCurrentEventloop()).withInspector(new AsyncHttpClient.JmxInspector());
	}

	@Test
	public void testMultiLineHeader() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> HttpResponse.ok200()
								.withHeader(DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
								.withHeader(CONTENT_TYPE, "text/\n          html")
								.withBody(wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>")))
				.withListenPort(port)
				.withAcceptOnce();
		server.listen();

		await(client.request(HttpRequest.get(url))
				.then(response -> response.loadBody()
						.whenComplete(TestUtils.assertCompleteFn(body -> {
							assertEquals("text/           html", response.getHeader(CONTENT_TYPE));
							assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", body.getString(UTF_8));
						}))));
	}

	@Test
	public void testGzipCompression() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> HttpResponse.ok200()
								.withBodyGzipCompression()
								.withBody(encodeAscii("Test message")))
				.withListenPort(port)
				.withAcceptOnce();

		server.listen();

		await(client.request(HttpRequest.get(url)
						.withHeader(ACCEPT_ENCODING, "gzip"))
				.then(response -> response.loadBody()
						.whenComplete(TestUtils.assertCompleteFn(body -> {
							assertEquals("Test message", body.getString(UTF_8));
							assertNotNull(response.getHeader(CONTENT_ENCODING));
						}))));
	}

	@Test
	public void testClientWithMaxKeepAliveRequests() throws Exception {
		client.withKeepAliveTimeout(Duration.ofSeconds(1));
		client.withMaxKeepAliveRequests(5);

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> HttpResponse.ok200())
				.withListenPort(port);
		server.listen();

		assertNotNull(client.getStats());
		checkMaxKeepAlive(5, server, client.getStats().getConnected());
	}

	@Test
	public void testServerWithMaxKeepAliveRequests() throws Exception {
		client.withKeepAliveTimeout(Duration.ofSeconds(1));

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.withMaxKeepAliveRequests(5);
		server.listen();

		assertNotNull(server.getAccepts());
		checkMaxKeepAlive(5, server, server.getAccepts());
	}

	@Test
	public void testServerWithNoKeepAlive() throws Exception {
		client.withKeepAliveTimeout(Duration.ofSeconds(30));

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.withKeepAliveTimeout(Duration.ZERO);
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

		SocketSettings socketSettings = SocketSettings.create()
				.withSendBufferSize(MemSize.of(1))
				.withReceiveBufferSize(MemSize.of(1))
				.withImplReadBufferSize(MemSize.of(1));

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withSocketSettings(socketSettings);

		// regular
		doTestHugeStreams(client, socketSettings, size, httpMessage -> httpMessage.addHeader(CONTENT_LENGTH, String.valueOf(size)));

		// chunked
		doTestHugeStreams(client, socketSettings, size, httpMessage -> {});

		// gzipped + chunked
		doTestHugeStreams(client, socketSettings, size, HttpMessage::setBodyGzipCompression);
	}

	@Test
	@Ignore("Takes a long time")
	public void testContentLengthPastMaxInt() throws IOException {
		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());

		Checksum inChecksum = new CRC32();
		Checksum outChecksum = new CRC32();

		long size = 3_000_000_000L;

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> HttpResponse.ok200()
								.withHeader(CONTENT_LENGTH, String.valueOf(size))
								.withBodyStream(request.getBodyStream()))
				.withListenPort(port);
		server.listen();

		HttpRequest request = HttpRequest.post("http://127.0.0.1:" + port)
				.withHeader(CONTENT_LENGTH, String.valueOf(size))
				.withBodyStream(ChannelSupplier.ofStream(Stream.generate(() -> {
					int length = 1_000_000;
					byte[] temp = new byte[length];
					RANDOM.nextBytes(temp);
					outChecksum.update(temp, 0, length);
					return ByteBuf.wrapForReading(temp);
				}).limit(3_000)));

		await(client.request(request)
				.then(response -> response.getBodyStream()
						.streamTo(ChannelConsumer.ofConsumer(buf -> {
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
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> {
							byte[] bytes = new byte[size];
							RANDOM.nextBytes(bytes);
							expected.put(bytes);
							return HttpResponse.ok200()
									.withBodyGzipCompression()
									.withBodyStream(ChannelSupplier.of(expected.slice()));
						})
				.withAcceptOnce()
				.withListenPort(port);

		server.listen();

		ByteBuf result = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop()).request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.getBodyStream().toCollector(ByteBufs.collector()))
				.whenComplete(server::close));
		assertArrayEquals(expected.asArray(), result.asArray());
	}

	@Test
	public void testEmptyRequestResponse() {
		List<Consumer<HttpMessage>> messageDecorators = asList(
				message -> {},
				HttpMessage::setBodyGzipCompression,
				message -> message.addHeader(CONTENT_LENGTH, "0"),
				message -> {
					message.setBodyGzipCompression();
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> message.setBody(ByteBuf.empty()),
				message -> {
					message.setBody(ByteBuf.empty());
					message.setBodyGzipCompression();
				},
				message -> {
					message.setBody(ByteBuf.empty());
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> {
					message.setBody(ByteBuf.empty());
					message.setBodyGzipCompression();
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> message.setBodyStream(ChannelSupplier.of()),
				message -> {
					message.setBodyStream(ChannelSupplier.of());
					message.setBodyGzipCompression();
				},
				message -> {
					message.setBodyStream(ChannelSupplier.of());
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> {
					message.setBodyStream(ChannelSupplier.of());
					message.setBodyGzipCompression();
					message.addHeader(CONTENT_LENGTH, "0");
				}
		);

		doTestEmptyRequestResponsePermutations(messageDecorators);
	}

	@Test
	public void testHugeUrls() throws IOException {
		char[] chars = new char[16 * 1024];
		Arrays.fill(chars, 'a');

		client.withKeepAliveTimeout(Duration.ofSeconds(30));

		AsyncHttpServer.JmxInspector inspector = new AsyncHttpServer.JmxInspector();
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> HttpResponse.ok200())
				.withListenPort(port)
				.withInspector(inspector)
				.withAcceptOnce(true);
		server.listen();

		HttpResponse response = await(client.request(HttpRequest.get(url + '/' + new String(chars))));

		assertEquals(400, response.getCode());

		ExceptionStats httpErrors = inspector.getHttpErrors();
		assertEquals(1, httpErrors.getTotal());
		Throwable lastException = httpErrors.getLastException();
		assert lastException != null;
		assertThat(lastException, instanceOf(MalformedHttpException.class));
		assertThat(lastException.getMessage(), containsString("Header line exceeds max header size"));
	}

	@Test
	public void testKeepAliveWithStreamingResponse() throws Exception {
		AsyncHttpClient.JmxInspector clientInspector = new AsyncHttpClient.JmxInspector();
		AsyncHttpServer.JmxInspector serverInspector = new AsyncHttpServer.JmxInspector();

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> HttpResponse.ok200()
								.withBodyStream(ChannelSupplier.of(ByteBuf.wrapForReading(HELLO_WORLD))))
				.withInspector(serverInspector)
				.withListenPort(port);

		server.listen();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withKeepAliveTimeout(Duration.ofSeconds(10))
				.withInspector(clientInspector);

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
		AsyncHttpClient.JmxInspector clientInspector = new AsyncHttpClient.JmxInspector();
		AsyncHttpServer.JmxInspector serverInspector = new AsyncHttpServer.JmxInspector();

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> request.loadBody()
								.whenComplete(assertCompleteFn(body -> assertEquals(decodeAscii(HELLO_WORLD), body.getString(UTF_8))))
								.map($ -> HttpResponse.ok200()
										.withBody(ByteBuf.wrapForReading(HELLO_WORLD))))
				.withInspector(serverInspector)
				.withListenPort(port);

		server.listen();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withKeepAliveTimeout(Duration.ofSeconds(10))
				.withInspector(clientInspector);

		assertEquals(0, client.getConnectionsKeepAliveCount());
		assertEquals(0, clientInspector.getConnected().getTotalCount());

		assertEquals(0, server.getConnectionsKeepAliveCount());
		assertEquals(0, serverInspector.getTotalConnections().getTotalCount());

		String url = "http://127.0.0.1:" + port;
		await(client.request(HttpRequest.get(url)
						.withBodyStream(ChannelSupplier.of(ByteBuf.wrapForReading(HELLO_WORLD))))
				.then(ensureHelloWorldAsyncFn())
				.whenResult(() -> {
					assertEquals(1, client.getConnectionsKeepAliveCount());
					assertEquals(1, clientInspector.getConnected().getTotalCount());

					assertEquals(1, server.getConnectionsKeepAliveCount());
					assertEquals(1, serverInspector.getTotalConnections().getTotalCount());
				})
				.then(() -> client.request(HttpRequest.get(url)
						.withBodyStream(ChannelSupplier.of(ByteBuf.wrapForReading(HELLO_WORLD)))))
				.then(ensureHelloWorldAsyncFn())
				.whenResult(() -> {
					assertEquals(1, client.getConnectionsKeepAliveCount());
					assertEquals(1, clientInspector.getConnected().getTotalCount());

					assertEquals(1, server.getConnectionsKeepAliveCount());
					assertEquals(1, serverInspector.getTotalConnections().getTotalCount());
				})
				.whenComplete(server::close));
	}

	private @NotNull FunctionEx<HttpResponse, Promise<? extends ByteBuf>> ensureHelloWorldAsyncFn() {
		return response -> response.loadBody()
				.whenComplete(assertCompleteFn(body -> assertEquals(decodeAscii(HELLO_WORLD), body.getString(UTF_8))))
				.then(AbstractHttpConnectionTest::post);
	}

	private void doTestEmptyRequestResponsePermutations(List<Consumer<HttpMessage>> messageDecorators) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		for (int i = 0; i < messageDecorators.size(); i++) {
			for (int j = 0; j < messageDecorators.size(); j++) {
				try {
					Consumer<HttpMessage> responseDecorator = messageDecorators.get(j);
					AsyncHttpServer server = AsyncHttpServer.create(eventloop,
									$ -> {
										HttpResponse response = HttpResponse.ok200();
										responseDecorator.accept(response);
										return response;
									})
							.withListenPort(port)
							.withAcceptOnce(true);

					try {
						server.listen();
					} catch (IOException e) {
						throw new AssertionError(e);
					}

					HttpRequest request = HttpRequest.post(url);
					messageDecorators.get(i).accept(request);

					String responseText = await(client.request(request)
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

	private void doTestHugeStreams(AsyncHttpClient client, SocketSettings socketSettings, int size, Consumer<HttpMessage> decorator) throws IOException {
		ByteBuf expected = ByteBufPool.allocate(size);
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> request.loadBody()
								.map(body -> {
									HttpResponse httpResponse = HttpResponse.ok200()
											.withBodyStream(request.getBodyStream()
													.transformWith(ChannelByteChunker.create(MemSize.of(1), MemSize.of(1))));
									decorator.accept(httpResponse);
									return httpResponse;
								}))
				.withListenPort(port)
				.withSocketSettings(socketSettings);
		server.listen();

		HttpRequest request = HttpRequest.post("http://127.0.0.1:" + port)
				.withBodyStream(ChannelSupplier.ofStream(Stream.generate(() -> {
					byte[] temp = new byte[1];
					RANDOM.nextBytes(temp);
					ByteBuf buf = ByteBuf.wrapForReading(temp);
					expected.put(buf.slice());
					return buf;
				}).limit(size)));

		decorator.accept(request);

		ByteBuf result = await(client.request(request)
				.then(response -> response.getBodyStream()
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
	private void checkMaxKeepAlive(int maxKeepAlive, AsyncHttpServer server, EventStats connectionCount) {
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
		Eventloop.getCurrentEventloop().post(() -> cb.set(value));
		return cb;
	}

	private void resetPort() {
		port = getFreePort();
		url = "http://127.0.0.1:" + port;
	}
}
