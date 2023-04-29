package io.activej.http;

import io.activej.async.exception.AsyncTimeoutException;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.ref.Ref;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.HttpClient.JmxInspector;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.net.SimpleServer;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettableCallback;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.https.SslUtils.createTestSslContext;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class HttpClientTest {
	private static final byte[] HELLO_WORLD = encodeAscii("Hello, World!");

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private int port;

	public void startServer() throws IOException {
		HttpServer.builder(Reactor.getCurrentReactor(),
						request -> HttpResponse.Builder.ok200()
								.withBodyStream(ChannelSuppliers.ofStream(
										IntStream.range(0, HELLO_WORLD.length)
												.mapToObj(idx -> {
													ByteBuf buf = ByteBufPool.allocate(1);
													buf.put(HELLO_WORLD[idx]);
													return buf;
												})))
								.toPromise())
				.withListenPort(port)
				.withAcceptOnce()
				.build()
				.listen();
	}

	@Before
	public void setUp() {
		port = getFreePort();
	}

	@Test
	public void testAsyncClient() throws Exception {
		startServer();

		IHttpClient client = HttpClient.create(Reactor.getCurrentReactor());
		await(client.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody()
						.whenComplete(assertCompleteFn(body -> assertEquals(decodeAscii(HELLO_WORLD), body.getString(UTF_8))))));
	}

	@Test
	@Ignore("Requires DNS look up, may flood remote server")
	public void testClientTimeoutConnect() {
		IHttpClient client = HttpClient.builder(Reactor.getCurrentReactor())
				.withConnectTimeout(Duration.ofMillis(1))
				.build();
		Exception e = awaitException(client.request(HttpRequest.get("http://google.com")));
		assertThat(e, instanceOf(HttpException.class));
		assertThat(e.getCause(), instanceOf(AsyncTimeoutException.class));
	}

	@Test
	public void testBigHttpMessage() throws IOException {
		startServer();

		int maxBodySize = HELLO_WORLD.length - 1;

		IHttpClient client = HttpClient.create(Reactor.getCurrentReactor());
		MalformedHttpException e = awaitException(client.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody(maxBodySize)));
		assertThat(e.getMessage(), containsString("HTTP body size exceeds load limit " + maxBodySize));
	}

	@Test
	public void testEmptyLineResponse() throws IOException {
		SimpleServer.builder(Reactor.getCurrentReactor(), socket ->
						socket.read()
								.whenResult(ByteBuf::recycle)
								.then(() -> socket.write(wrapAscii("\r\n")))
								.whenComplete(socket::close))
				.withListenPort(port)
				.withAcceptOnce()
				.build()
				.listen();

		IHttpClient client = HttpClient.create(Reactor.getCurrentReactor());
		Exception e = awaitException(client.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody()));

		assertThat(e, instanceOf(MalformedHttpException.class));
	}

	@Test
	public void testActiveRequestsCounter() throws IOException {
		NioReactor reactor = Reactor.getCurrentReactor();

		List<SettableCallback<HttpResponse>> responses = new ArrayList<>();

		HttpServer server = HttpServer.builder(reactor,
						request -> Promise.ofCallback(responses::add))
				.withListenPort(port)
				.build();

		server.listen();

		JmxInspector inspector = new JmxInspector();
		IHttpClient httpClient = HttpClient.builder(reactor)
				.withNoKeepAlive()
				.withConnectTimeout(Duration.ofMillis(20))
				.withReadWriteTimeout(Duration.ofMillis(20))
				.withInspector(inspector)
				.build();

		Exception e = awaitException(Promises.all(
						httpClient.request(HttpRequest.get("http://127.0.0.1:" + port)),
						httpClient.request(HttpRequest.get("http://127.0.0.1:" + port)),
						httpClient.request(HttpRequest.get("http://127.0.0.1:" + port)),
						httpClient.request(HttpRequest.get("http://127.0.0.1:" + port)),
						httpClient.request(HttpRequest.get("http://127.0.0.1:" + port)))
				.whenComplete(() -> {
					server.close();
					responses.forEach(response -> response.set(HttpResponse.ok200()));

					inspector.getTotalRequests().refresh(reactor.currentTimeMillis());
					inspector.getHttpTimeouts().refresh(reactor.currentTimeMillis());

					System.out.println(inspector.getTotalRequests().getTotalCount());
					System.out.println();
					System.out.println(inspector.getHttpTimeouts().getTotalCount());
					System.out.println(inspector.getResolveErrors().getTotal());
					System.out.println(inspector.getConnectErrors().getTotal());
					System.out.println(inspector.getTotalResponses());

					assertEquals(4, inspector.getActiveRequests());
				}));
		assertThat(e, instanceOf(AsyncTimeoutException.class));
	}

	@Test
	public void testActiveRequestsCounterWithoutRefresh() throws IOException {
		NioReactor reactor = Reactor.getCurrentReactor();

		HttpServer.builder(reactor, request -> HttpResponse.ok200().toPromise())
				.withAcceptOnce()
				.withListenPort(port)
				.build()
				.listen();

		JmxInspector inspector = new JmxInspector();
		IHttpClient httpClient = HttpClient.builder(reactor)
				.withInspector(inspector)
				.build();

		Promise<HttpResponse> requestPromise = httpClient.request(HttpRequest.get("http://127.0.0.1:" + port));
		assertEquals(1, inspector.getActiveRequests());
		await(requestPromise);
		assertEquals(0, inspector.getActiveRequests());
	}

	@Test
	public void testClientNoContentLength() throws Exception {
		String text = "content";
		ByteBuf req = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\r\n\r\n" + text));
		String responseText = await(customResponse(req, false)
				.then(response -> response.loadBody())
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testClientNoContentLengthSSL() throws Exception {
		String text = "content";
		ByteBuf req = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\r\n\r\n" + text));
		String responseText = await(customResponse(req, false)
				.then(response -> response.loadBody())
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testClientNoContentLengthGzipped() throws Exception {
		String text = "content";
		ByteBuf headLines = ByteBuf.wrapForReading(encodeAscii("""
				HTTP/1.1 200 OK\r
				Content-Encoding: gzip\r
				\r
				"""));

		String responseText = await(customResponse(ByteBufPool.append(headLines, GzipProcessorUtils.toGzip(wrapAscii(text))), false)
				.then(response -> response.loadBody())
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testClientNoContentLengthGzippedSSL() throws Exception {
		String text = "content";
		ByteBuf headLines = ByteBuf.wrapForReading(encodeAscii("""
				HTTP/1.1 200 OK\r
				Content-Encoding: gzip\r
				\r
				"""));

		String responseText = await(customResponse(ByteBufPool.append(headLines, GzipProcessorUtils.toGzip(wrapAscii(text))), true)
				.then(response -> response.loadBody())
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testAsyncPipelining() throws IOException {
		ServerSocket listener = new ServerSocket(port);
		Ref<Socket> socketRef = new Ref<>();
		new Thread(() -> {
			while (Thread.currentThread().isAlive()) {
				try {
					Socket socket = listener.accept();
					socketRef.set(socket);
					DataInputStream in = new DataInputStream(socket.getInputStream());
					int b = 0;
					//noinspection StatementWithEmptyBody
					while (b != -1 && !(((b = in.read()) == CR || b == LF) && (b = in.read()) == LF)) {
					}
					ByteBuf buf = ByteBuf.wrapForReading(encodeAscii("""
							HTTP/1.1 200 OK
							Content-Length:  4

							testHTTP/1.1 200 OK
							Content-Length:  4

							test"""));
					socket.getOutputStream().write(buf.array(), buf.head(), buf.readRemaining());
				} catch (IOException ignored) {
				}
			}
		}).start();

		IHttpClient client = HttpClient.builder(Reactor.getCurrentReactor())
				.withKeepAliveTimeout(Duration.ofSeconds(30))
				.build();

		int code = await(client
				.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody().async()
						.then(() -> client.request(HttpRequest.get("http://127.0.0.1:" + port)))
						.then(res -> {
							assertFalse(res.isRecycled());
							return res.loadBody()
									.map(body -> {
										assertEquals("test", body.getString(UTF_8));
										return res;
									});
						})
						.map(HttpResponse::getCode)
						.whenComplete(assertingFn(($, e) -> {
							socketRef.get().close();
							listener.close();
						}))));

		assertEquals(200, code);
	}

	@Test
	public void testResponseWithoutReasonPhrase() throws IOException {
		ByteBuf req = ByteBuf.wrapForReading(encodeAscii("""
				HTTP/1.1 200
				Content-Length: 0\r
				\r
				"""));
		assertEquals((Integer) 200, await(customResponse(req, false).map(HttpResponse::getCode)));
	}

	@Test
	public void testActiveConnectionsCountWithFailingServer() throws IOException {
		String serverResponse = "\r\n";
		SimpleServer.builder(Reactor.getCurrentReactor(), socket ->
						socket.read()
								.whenResult(ByteBuf::recycle)
								.then(() -> socket.write(wrapAscii(serverResponse)))
								.whenComplete(socket::close))
				.withListenPort(port)
				.withAcceptOnce()
				.build()
				.listen();

		JmxInspector inspector = new JmxInspector();
		IHttpClient client = HttpClient.builder(Reactor.getCurrentReactor())
				.withInspector(inspector)
				.build();

		Exception e = awaitException(client.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody()));

		assertThat(e, instanceOf(MalformedHttpException.class));

		assertEquals(0, inspector.getActiveConnections());

		ExceptionStats malformedHttpExceptions = inspector.getMalformedHttpExceptions();
		assertEquals(1, malformedHttpExceptions.getTotal());
		assertEquals("Invalid response", malformedHttpExceptions.getLastMessage());
		String context = (String) malformedHttpExceptions.getContext();
		assertNotNull(context);
		assertFalse(context.isEmpty());
		assertTrue(serverResponse.startsWith(context));
	}

	private static final ByteBufsDecoder<ByteBuf> REQUEST_DECODER = bufs -> {
		for (int i = 0; i < bufs.remainingBytes() - 3; i++) {
			if (bufs.peekByte(i) == CR &&
					bufs.peekByte(i + 1) == LF &&
					bufs.peekByte(i + 2) == CR &&
					bufs.peekByte(i + 3) == LF) {
				return bufs.takeRemaining();
			}
		}
		return null;
	};

	private Promise<HttpResponse> customResponse(ByteBuf rawResponse, boolean ssl) throws IOException {
		SimpleServer.Builder builder = SimpleServer.builder(Reactor.getCurrentReactor(), socket ->
						BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
								.decode(REQUEST_DECODER)
								.whenResult(ByteBuf::recycle)
								.then(() -> socket.write(rawResponse))
								.whenResult(socket::close))
				.withAcceptOnce();
		if (ssl) {
			builder.withSslListenAddress(createTestSslContext(), Executors.newSingleThreadExecutor(), new InetSocketAddress(port));
		} else {
			builder.withListenAddress(new InetSocketAddress(port));
		}
		builder.build().listen();
		return HttpClient.builder(Reactor.getCurrentReactor())
				.withSslEnabled(createTestSslContext(), Executors.newSingleThreadExecutor())
				.build()
				.request(HttpRequest.get("http" + (ssl ? "s" : "") + "://127.0.0.1:" + port));
	}

}
