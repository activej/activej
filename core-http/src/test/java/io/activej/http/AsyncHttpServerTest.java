package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.Selector;
import java.util.LinkedHashSet;
import java.util.Random;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.http.TestUtils.readFully;
import static io.activej.http.TestUtils.toByteArray;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class AsyncHttpServerTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static AsyncHttpServer blockingHttpServer(Eventloop primaryEventloop, int port) {
		return AsyncHttpServer.create(primaryEventloop,
				request ->
						HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery())))
				.withListenPort(port);
	}

	public static AsyncHttpServer asyncHttpServer(Eventloop primaryEventloop, int port) {
		return AsyncHttpServer.create(primaryEventloop,
				request ->
						Promise.ofCallback(cb -> cb.post(
								HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery())))))
				.withListenPort(port);
	}

	static final Random RANDOM = new Random();

	public static AsyncHttpServer delayedHttpServer(Eventloop primaryEventloop, int port) {
		return AsyncHttpServer.create(primaryEventloop,
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = getFreePort();

		doTestKeepAlive_Http_1_0(eventloop, blockingHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_0(eventloop, asyncHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_0(eventloop, delayedHttpServer(eventloop, port), port);
	}

	private void doTestKeepAlive_Http_1_0(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
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
	}

	@Test
	public void testKeepAlive_Http_1_1() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = getFreePort();

		doTestKeepAlive_Http_1_1(eventloop, blockingHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_1(eventloop, asyncHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_1(eventloop, delayedHttpServer(eventloop, port), port);
	}

	private void doTestKeepAlive_Http_1_1(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
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
	}

	@Test
	public void testClosed() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = getFreePort();
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = getFreePort();
		SettablePromise<Throwable> throwablePromise = new SettablePromise<>();
		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(() -> Promise.of(wrapAscii("Hello")), throwablePromise::set);
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
		Throwable throwable = await(throwablePromise);
		assertThat(throwable, instanceOf(IOException.class));
	}

	@Test
	public void testNoKeepAlive_Http_1_0() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = getFreePort();
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = getFreePort();
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		int port = getFreePort();
//		doTestPipelining(eventloop, blockingHttpServer(eventloop));
//		doTestPipelining(eventloop, asyncHttpServer(eventloop));
		doTestPipelining(eventloop, delayedHttpServer(eventloop, port), port);
	}

	private void doTestPipelining(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n"
					+ "GET /123456 HTTP/1.1\r\nHost: localhost\r\n\r\n" +
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
	}

	@Test
	@Ignore("does not work")
	public void testPipelining2() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		int port = getFreePort();
//		doTestPipelining(eventloop, blockingHttpServer(eventloop));
//		doTestPipelining(eventloop, asyncHttpServer(eventloop));
		doTestPipelining2(eventloop, delayedHttpServer(eventloop, port), port);
	}

	private void doTestPipelining2(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.0\r\nHost: localhost\r\n\r\n"
					+ "GET /123456 HTTP/1.1\r\nHost: localhost\r\n\r\n" +
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
	}

	@Test
	public void testBigHttpMessage() throws Exception {
		int port = getFreePort();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

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
//		assertEquals(AbstractHttpConnection.TOO_BIG_HTTP_MESSAGE,
//				server.getStats().getHttpErrors().getLastException());
	}

	@Test
	public void testExpectContinue() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		int port = getFreePort();
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
		int port = getFreePort();

		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> {
					// imitate network problems
					shutdownAllChannels(eventloop);
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

	private static void shutdownAllChannels(Eventloop eventloop) {
		try {
			Selector selector = eventloop.getSelector();
			assert selector != null;
			selector.keys().iterator().next().channel().close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		AsyncHttpServer server = blockingHttpServer(eventloop, 8888);
		server.listen();
		eventloop.run();
	}
}
