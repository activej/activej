package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.Selector;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.TestUtils.readFully;
import static io.activej.http.TestUtils.toByteArray;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class AsyncHttpServerTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private Eventloop eventloop;
	private int port;

	@Before
	public void setUp() {
		eventloop = Eventloop.getCurrentEventloop();
		port = getFreePort();
	}

	public AsyncHttpServer blockingHttpServer() {
		return AsyncHttpServer.create(eventloop,
				request ->
						HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery())))
				.withListenPort(port);
	}

	public AsyncHttpServer asyncHttpServer() {
		return AsyncHttpServer.create(eventloop,
				request ->
						Promise.ofCallback(cb -> cb.post(
								HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery())))))
				.withListenPort(port);
	}

	static final Random RANDOM = new Random();

	public AsyncHttpServer delayedHttpServer() {
		return AsyncHttpServer.create(eventloop,
				request -> Promises.delay(RANDOM.nextInt(3),
						HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()))))
				.withListenPort(port);
	}

	public static void writeByRandomParts(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		Random random = new Random();
		while (buf.canRead()) {
			int count = min(1 + random.nextInt(5), buf.readRemaining());
			socket.getOutputStream().write(buf.array(), buf.head(), count);
			buf.moveHead(count);
		}
	}

	public static void readAndAssert(InputStream is, String expected) {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		String actual = decodeAscii(bytes);
		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(actual.split("\r\n"))));
	}

	@Test
	public void testKeepAlive_Http_1_0() throws Exception {
		doTestKeepAlive_Http_1_0(blockingHttpServer());
		doTestKeepAlive_Http_1_0(asyncHttpServer());
		doTestKeepAlive_Http_1_0(delayedHttpServer());
	}

	private void doTestKeepAlive_Http_1_0(AsyncHttpServer server) throws Exception {
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 200; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.0\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		}

		writeByRandomParts(socket, "GET /abc HTTP/1.0\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc"); // ?

		assertEquals(0, toByteArray(socket.getInputStream()).length);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
		resetPort();
	}

	@Test
	public void testKeepAlive_Http_1_1() throws Exception {
		doTestKeepAlive_Http_1_1(blockingHttpServer());
		doTestKeepAlive_Http_1_1(asyncHttpServer());
		doTestKeepAlive_Http_1_1(delayedHttpServer());
	}

	private void doTestKeepAlive_Http_1_1(AsyncHttpServer server) throws Exception {
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 200; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nHost: localhost\r\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		}

		writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc"); // ?

		assertEquals(0, toByteArray(socket.getInputStream()).length);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
		resetPort();
	}

	@Test
	public void testClosed() throws Exception {
		AsyncHttpServer server = blockingHttpServer();
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n");
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testBodySupplierClosingOnDisconnect() throws Exception {
		SettablePromise<Exception> exceptionPromise = new SettablePromise<>();
		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(() -> Promise.of(wrapAscii("Hello")), exceptionPromise::set);
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, req -> HttpResponse.ok200().withBodyStream(supplier))
				.withListenPort(port)
				.withAcceptOnce();
		server.listen();
		new Thread(() -> {
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("localhost", port));
				writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nHost: localhost\r\n\r\n");
				socket.close();
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}).start();
		Exception exception = await(exceptionPromise);
		assertThat(exception, instanceOf(IOException.class));
	}

	@Test
	public void testNoKeepAlive_Http_1_0() throws Exception {
		AsyncHttpServer server = blockingHttpServer();
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "GET /abc HTTP/1.0\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc");
		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testNoKeepAlive_Http_1_1() throws Exception {
		AsyncHttpServer server = blockingHttpServer();
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nConnection: close\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc");
		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testPipelining() throws Exception {
		doTestPipelining(blockingHttpServer());
		doTestPipelining(asyncHttpServer());
		doTestPipelining(delayedHttpServer());
	}

	private void doTestPipelining(AsyncHttpServer server) throws Exception {
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "" +
					"GET /abc HTTP/1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n" +
					"GET /123456 HTTP/1.1\r\nHost: localhost\r\n\r\n" +

					"POST /post1 HTTP/1.1\r\n" +
					"Host: localhost\r\n" +
					"Content-Length: 8\r\n" +
					"Content-Type: application/json\r\n\r\n" +
					"{\"at\":2}" +

					"POST /post2 HTTP/1.1\r\n" +
					"Host: localhost\r\n" +
					"Content-Length: 8\r\n" +
					"Content-Type: application/json\r\n\r\n" +
					"{\"at\":2}" +

					"");
		}

		for (int i = 0; i < 100; i++) {
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 7\r\n\r\n/123456");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\n/post1");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\n/post2");
		}

		server.closeFuture().get();
		thread.join();
		resetPort();
	}

	@Test
	public void testBigHttpMessage() throws Exception {
		byte[] body = encodeAscii("Test big HTTP message body");
		HttpRequest request = HttpRequest.post("http://127.0.0.1:" + port)
				.withBody(body);

		ByteBuf buf = ByteBufPool.allocate(request.estimateSize() + body.length);
		request.writeTo(buf);
		buf.put(body);

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				req -> HttpResponse.ok200()
						.withBody(encodeAscii(req.getUrl().getPathAndQuery())))
				.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress("localhost", port));
			socket.getOutputStream().write(buf.array(), buf.head(), buf.readRemaining());
			buf.recycle();
			Thread.sleep(100);
		}
		server.closeFuture().get();
		thread.join();
//		assertEquals(1, server.getStats().getHttpErrors().getTotal());
//		System.out.println(server.getStats().getHttpErrors().getLastException());
//		assertEquals(AbstractHttpConnection.TOO_BIG_HTTP_MESSAGE,
//				server.getStats().getHttpErrors().getLastException());
	}

	@Test
	public void testExpectContinue() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())))
				.withListenPort(port);

		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress("localhost", port));

		writeByRandomParts(socket, "POST /abc HTTP/1.0\r\nHost: localhost\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 100 Continue\r\n\r\n");

		writeByRandomParts(socket, "abcde");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nabcde");

		assertEquals(0, toByteArray(socket.getInputStream()).length);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testBodyRecycledOnce() throws IOException, InterruptedException {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> {
					// imitate network problems
					shutdownAllChannels();
					return HttpResponse.ok200();
				})
				.withListenPort(port)
				.withAcceptOnce(true);

		server.listen();

		Thread thread = new Thread(() -> {
			try (Socket socket = new Socket()) {
				socket.connect(new InetSocketAddress("localhost", port));
				ByteBuf buf = ByteBuf.wrapForReading(encodeAscii("GET /  HTTP/1.1\r\nHost: localhost\r\n" +
						"Connection: close\r\nContent-Length: 10\r\n\r\ntest"));
				socket.getOutputStream().write(buf.array(), buf.head(), buf.readRemaining());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		thread.start();
		eventloop.run();
		thread.join();
	}

	@Test
	public void testPostParameters() throws IOException, ExecutionException, InterruptedException {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, request ->
				request.loadBody()
						.then(() -> {
							Map<String, String> postParameters = request.getPostParameters();
							StringBuilder sb = new StringBuilder();
							for (Map.Entry<String, String> entry : postParameters.entrySet()) {
								sb.append(entry.getKey())
										.append("=")
										.append(entry.getValue())
										.append(";");
							}

							return Promise.of(HttpResponse.ok200().withBody(encodeAscii(sb.toString())));
						}));
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "POST / HTTP/1.1\r\n" +
				"Host: localhost\r\n" +
				"Connection: close\r\n" +
				"Content-Type: application/x-www-form-urlencoded\r\n" +
				"Content-Length: 27\r\n\r\n" +
				"field1=value1&field2=value2");

		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\n" +
				"Connection: close\r\n" +
				"Content-Length: 28\r\n\r\n" +
				"field1=value1;field2=value2;");
		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testIncompleteRequest() throws IOException, ExecutionException, InterruptedException {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, request ->
				request.loadBody()
						.map(($, e) -> {
							assertTrue(e instanceof MalformedHttpException);

							assertFalse(request.isRecycled());
							assertTrue(request.getConnection().isClosed());

							assertEquals(request.getHeader(HttpHeaders.HOST), "localhost");
							return HttpResponse.ofCode(400);
						}));
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "POST / HTTP/1.1\r\n" +
				"Host: localhost\r\n" +
				"Connection: close\r\n" +
				"Content-Type: application/x-www-form-urlencoded\r\n" +
				"Content-Length: 100\r\n\r\n" +
				"field1=value1&field2=value2");
		socket.shutdownOutput();

		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	private void shutdownAllChannels() {
		try {
			Selector selector = eventloop.getSelector();
			assert selector != null;
			selector.keys().iterator().next().channel().close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void resetPort() {
		port = getFreePort();
	}
}
