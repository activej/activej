package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.UnknownFormatException;
import io.activej.common.ref.Ref;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient.JmxInspector;
import io.activej.net.SimpleServer;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
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
import static io.activej.eventloop.Eventloop.CONNECT_TIMEOUT;
import static io.activej.http.AbstractHttpConnection.READ_TIMEOUT_ERROR;
import static io.activej.http.HttpClientConnection.INVALID_RESPONSE;
import static io.activej.https.SslUtils.createTestSslContext;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public final class AsyncHttpClientTest {
	private static final int PORT = getFreePort();

	private static final byte[] HELLO_WORLD = encodeAscii("Hello, World!");

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static void startServer() throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> HttpResponse.ok200()
						.withBodyStream(ChannelSupplier.ofStream(
								IntStream.range(0, HELLO_WORLD.length)
										.mapToObj(idx -> {
											ByteBuf buf = ByteBufPool.allocate(1);
											buf.put(HELLO_WORLD[idx]);
											return buf;
										}))))
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();
	}

	@Test
	public void testAsyncClient() throws Exception {
		startServer();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
		await(client.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.then(response -> response.loadBody()
						.whenComplete(assertComplete(body -> assertEquals(decodeAscii(HELLO_WORLD), body.getString(UTF_8))))));
	}

	@Test
	@Ignore
	public void testClientTimeoutConnect() {
		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withConnectTimeout(Duration.ofMillis(1));
		AsyncTimeoutException e = awaitException(client.request(HttpRequest.get("http://google.com")));
		assertSame(CONNECT_TIMEOUT, e);
	}

	@Test
	public void testBigHttpMessage() throws IOException {
		startServer();

		int maxBodySize = HELLO_WORLD.length - 1;

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
		InvalidSizeException e = awaitException(client.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.then(response -> response.loadBody(maxBodySize)));
		assertThat(e.getMessage(), containsString("HTTP body size exceeds load limit " + maxBodySize));
	}

	@Test
	public void testEmptyLineResponse() throws IOException {
		SimpleServer.create(socket ->
				socket.read()
						.whenResult(ByteBuf::recycle)
						.then(() -> socket.write(wrapAscii("\r\n")))
						.whenComplete(socket::close))
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
		UnknownFormatException e = awaitException(client.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.then(HttpMessage::loadBody));

		assertSame(INVALID_RESPONSE, e);
	}

	@Test
	public void testActiveRequestsCounter() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<SettablePromise<HttpResponse>> responses = new ArrayList<>();

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> Promise.ofCallback(responses::add))
				.withListenPort(PORT);

		server.listen();

		JmxInspector inspector = new JmxInspector();
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop)
				.withNoKeepAlive()
				.withConnectTimeout(Duration.ofMillis(20))
				.withReadWriteTimeout(Duration.ofMillis(20))
				.withInspector(inspector);

		AsyncTimeoutException e = awaitException(Promises.all(
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT)))
				.whenComplete(() -> {
					server.close();
					responses.forEach(response -> response.set(HttpResponse.ok200()));

					inspector.getTotalRequests().refresh(eventloop.currentTimeMillis());
					inspector.getHttpTimeouts().refresh(eventloop.currentTimeMillis());

					System.out.println(inspector.getTotalRequests().getTotalCount());
					System.out.println();
					System.out.println(inspector.getHttpTimeouts().getTotalCount());
					System.out.println(inspector.getResolveErrors().getTotal());
					System.out.println(inspector.getConnectErrors().getTotal());
					System.out.println(inspector.getTotalResponses());

					assertEquals(4, inspector.getActiveRequests());
				}));
		assertSame(READ_TIMEOUT_ERROR, e);
	}

	@Test
	public void testActiveRequestsCounterWithoutRefresh() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> HttpResponse.ok200())
				.withAcceptOnce()
				.withListenPort(PORT);

		server.listen();

		JmxInspector inspector = new JmxInspector();
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop)
				.withInspector(inspector);

		Promise<HttpResponse> requestPromise = httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT));
		assertEquals(1, inspector.getActiveRequests());
		await(requestPromise);
		assertEquals(0, inspector.getActiveRequests());
	}

	@Test
	public void testClientNoContentLength() throws Exception {
		String text = "content";
		ByteBuf req = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\r\n\r\n" + text));
		String responseText = await(customResponse(req, false)
				.then(HttpMessage::loadBody)
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testClientNoContentLengthSSL() throws Exception {
		String text = "content";
		ByteBuf req = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\r\n\r\n" + text));
		String responseText = await(customResponse(req, false)
				.then(HttpMessage::loadBody)
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testClientNoContentLengthGzipped() throws Exception {
		String text = "content";
		ByteBuf headLines = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\r\n" +
				"Content-Encoding: gzip\r\n\r\n"));

		String responseText = await(customResponse(ByteBufPool.append(headLines, GzipProcessorUtils.toGzip(wrapAscii(text))), false)
				.then(HttpMessage::loadBody)
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testClientNoContentLengthGzippedSSL() throws Exception {
		String text = "content";
		ByteBuf headLines = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\r\n" +
				"Content-Encoding: gzip\r\n\r\n"));

		String responseText = await(customResponse(ByteBufPool.append(headLines, GzipProcessorUtils.toGzip(wrapAscii(text))), true)
				.then(HttpMessage::loadBody)
				.map(byteBuf -> byteBuf.getString(UTF_8)));
		assertEquals(text, responseText);
	}

	@Test
	public void testAsyncPipelining() throws IOException {
		ServerSocket listener = new ServerSocket(PORT);
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
					ByteBuf buf = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200 OK\nContent-Length:  4\n\ntest" +
							"HTTP/1.1 200 OK\nContent-Length:  4\n\ntest"));
					socket.getOutputStream().write(buf.array(), buf.head(), buf.readRemaining());
				} catch (IOException ignored) {
				}
			}
		}).start();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withKeepAliveTimeout(Duration.ofSeconds(30));

		int code = await(client
				.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.then(response -> response.loadBody().async()
						.then(() -> client.request(HttpRequest.get("http://127.0.0.1:" + PORT)))
						.then(res -> {
							assertFalse(res.isRecycled());
							return res.loadBody()
									.map(body -> {
										assertEquals("test", body.getString(UTF_8));
										return res;
									});
						})
						.map(HttpResponse::getCode)
						.whenComplete(asserting(($, e) -> {
							socketRef.get().close();
							listener.close();
						}))));

		assertEquals(200, code);
	}

	@Test
	public void testResponseWithoutReasonPhrase() throws IOException {
		ByteBuf req = ByteBuf.wrapForReading(encodeAscii("HTTP/1.1 200\n" +
				"Content-Length: 0\r\n\r\n"));
		assertEquals((Integer) 200, await(customResponse(req, false).map(HttpResponse::getCode)));
	}

	private static final ByteBufsDecoder<ByteBuf> REQUEST_PARSER = bufs -> {
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
		SimpleServer server = SimpleServer.create(asyncTcpSocket ->
				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(asyncTcpSocket))
						.parse(REQUEST_PARSER)
						.whenResult(ByteBuf::recycle)
						.then(() -> asyncTcpSocket.write(rawResponse))
						.whenResult(asyncTcpSocket::close))
				.withAcceptOnce();
		if (ssl) {
			server.withSslListenAddress(createTestSslContext(), Executors.newSingleThreadExecutor(), new InetSocketAddress(PORT));
		} else {
			server.withListenAddress(new InetSocketAddress(PORT));
		}
		server.listen();
		return AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withSslEnabled(createTestSslContext(), Executors.newSingleThreadExecutor())
				.request(HttpRequest.get("http" + (ssl ? "s" : "") + "://127.0.0.1:" + PORT));
	}

}
