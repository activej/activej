package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.HttpServer.JmxInspector;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.test.TestUtils.getFreePort;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class HttpStreamTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public static final String CRLF = "\r\n";

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private final String requestBody = """
			Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor.
			Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.
			Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim.
			Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu.
			In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium.""";

	private List<ByteBuf> expectedList;
	private int port;

	@Before
	public void setUp() {
		port = getFreePort();
		expectedList = getBufsList(requestBody.getBytes());
	}

	@Test
	public void testStreamUpload() throws IOException {
		startTestServer(request -> request
				.takeBodyStream()
				.async()
				.toCollector(ByteBufs.collector())
				.whenComplete(TestUtils.assertCompleteFn(buf -> assertEquals(requestBody, buf.asString(UTF_8))))
				.then(s -> Promise.of(HttpResponse.ok200())));

		Integer code = await(HttpClient.create(Reactor.getCurrentReactor())
				.request(HttpRequest.Builder.post("http://127.0.0.1:" + port)
						.withBodyStream(ChannelSuppliers.ofList(expectedList)
								.mapAsync(item -> Promises.delay(200L, item)))
						.build())
				.async()
				.map(HttpResponse::getCode));

		assertEquals((Integer) 200, code);
	}

	@Test
	public void testStreamDownload() throws IOException {
		startTestServer(request ->
				HttpResponse.Builder.ok200()
						.withBodyStream(ChannelSuppliers.ofList(expectedList)
								.mapAsync(item -> Promises.delay(1L, item)))
						.toPromise());

		ByteBuf body = await(HttpClient.create(Reactor.getCurrentReactor())
				.request(HttpRequest.post("http://127.0.0.1:" + port))
				.async()
				.whenComplete(TestUtils.assertCompleteFn(response -> assertEquals(200, response.getCode())))
				.then(response -> response.takeBodyStream().async().toCollector(ByteBufs.collector())));

		assertEquals(requestBody, body.asString(UTF_8));
	}

	@Test
	public void testLoopBack() throws IOException {
		startTestServer(request -> request
				.takeBodyStream()
				.async()
				.toList()
				.map(ChannelSuppliers::ofList)
				.then(bodyStream -> Promise.of(HttpResponse.Builder.ok200().withBodyStream(bodyStream.async()).build())));

		ByteBuf body = await(HttpClient.create(Reactor.getCurrentReactor())
				.request(HttpRequest.Builder.post("http://127.0.0.1:" + port)
						.withBodyStream(ChannelSuppliers.ofList(expectedList)
								.mapAsync(item -> Promises.delay(1L, item)))
						.build())
				.whenComplete(TestUtils.assertCompleteFn(response -> assertEquals(200, response.getCode())))
				.then(response -> response.takeBodyStream().async().toCollector(ByteBufs.collector())));

		assertEquals(requestBody, body.asString(UTF_8));
	}

	@Test
	public void testCloseWithError() throws IOException {
		String exceptionMessage = "Test Exception";

		startTestServer(request -> Promise.ofException(new HttpError(432, exceptionMessage)));

		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.ofList(expectedList);

		ByteBuf body = await(HttpClient.create(Reactor.getCurrentReactor())
				.request(HttpRequest.Builder.post("http://127.0.0.1:" + port)
						.withBodyStream(supplier)
						.build())
				.then(response -> response.takeBodyStream().toCollector(ByteBufs.collector())));

		assertTrue(body.asString(UTF_8).contains(exceptionMessage));
	}

	@Test
	public void testChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.Builder.ok200().withBody(body.slice()).build()));

		String chunkedRequest =
				"POST / HTTP/1.1" + CRLF +
						"Host: localhost" + CRLF +
						"Transfer-Encoding: chunked" + CRLF + CRLF +
						"4" + CRLF + "Test" + CRLF + "0" + CRLF + CRLF;

		String responseMessage =
				"HTTP/1.1 200 OK" + CRLF +
						"Connection: keep-alive" + CRLF +
						"Content-Length: 4" + CRLF + CRLF +
						"Test";

		ByteBuf body = await(TcpSocket.connect(getCurrentReactor(), new InetSocketAddress(port))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then(() -> socket.write(null))
						.then(() -> ChannelSuppliers.ofSocket(socket).toCollector(ByteBufs.collector()))
						.whenComplete(socket::close)));

		assertEquals(responseMessage, body.asString(UTF_8));

		// not used here
		Recyclers.recycle(expectedList);
	}

	@Test
	public void testMalformedChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.Builder.ok200().withBody(body.slice()).build()));

		String chunkedRequest =
				"POST / HTTP/1.1" + CRLF +
						"Host: localhost" + CRLF +
						"Transfer-Encoding: chunked" + CRLF + CRLF +
						"ffffffffff";

		ByteBuf body = await(TcpSocket.connect(getCurrentReactor(), new InetSocketAddress(port))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then(socket::read)
						.whenComplete(socket::close)));

		assertNull(body);

//		String response = body.asString(UTF_8);
//		System.out.println(response);
//		assertTrue(response.contains("400"));
//		assertTrue(response.contains("Malformed chunk length"));

		// not used here
		Recyclers.recycle(expectedList);
	}

	@Test
	public void testTruncatedRequest() throws IOException {
		JmxInspector inspector = new JmxInspector();
		startTestServer(request -> request.loadBody()
						.map(body -> HttpResponse.Builder.ok200()
								.withBody(body.slice())
								.build()),
				inspector);

		String chunkedRequest =
				"POST / HTTP/1.1" + CRLF +
						"Host: localhost" + CRLF +
						"Content-Length: 13" + CRLF +
						"Transfer-Encoding: chunked" + CRLF + CRLF +
						"3";

		ByteBuf body = await(TcpSocket.connect(getCurrentReactor(), new InetSocketAddress(port))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then(() -> socket.write(null))
						.then(socket::read)
						.whenComplete(socket::close)));

		assertEquals(
				"HTTP/1.1 400 Bad Request\r\n" +
						"Connection: close\r\n" +
						"Content-Length: 0\r\n" +
						"\r\n",
				body.asString(UTF_8));

		assertEquals(1, inspector.getMalformedHttpExceptions().getTotal());
		assertEquals("Incomplete HTTP message", inspector.getMalformedHttpExceptions().getLastMessage());
		assertEquals(0, inspector.getHttpErrors().getTotal());

//		String response = body.asString(UTF_8);
//		assertTrue(response.contains("HTTP/1.1 400 Bad Request"));
//		assertTrue(response.contains("Incomplete HTTP message"));

		// not used here
		Recyclers.recycle(expectedList);
	}

	@Test
	public void testSendingErrors() throws IOException {
		Exception exception = new Exception("Test Exception");

		startTestServer(request -> request.loadBody().map(body -> HttpResponse.Builder.ok200().withBody(body.slice()).build()));

		Exception e = awaitException(
				HttpClient.create(Reactor.getCurrentReactor())
						.request(HttpRequest.Builder.post("http://127.0.0.1:" + port)
								.withBodyStream(ChannelSuppliers.concat(
										ChannelSuppliers.ofList(expectedList),
										ChannelSuppliers.ofException(exception)))
								.build())
						.then(response -> response.takeBodyStream().toCollector(ByteBufs.collector())));

		assertThat(e, instanceOf(HttpException.class));
		assertSame(exception, e.getCause());
	}

	private void startTestServer(AsyncServlet servlet) throws IOException {
		startTestServer(servlet, null);
	}

	private void startTestServer(AsyncServlet servlet, JmxInspector inspector) throws IOException {
		HttpServer.Builder builder = HttpServer.builder(Reactor.getCurrentReactor(), servlet)
				.withListenPort(port)
				.withAcceptOnce();
		if (inspector != null) {
			builder.withInspector(inspector);
		}
		builder.build().listen();
	}

	private List<ByteBuf> getBufsList(byte[] array) {
		List<ByteBuf> list = new ArrayList<>();
		ByteBuf buf = ByteBufPool.allocate(array.length);
		buf.put(array);
		int bufSize = ThreadLocalRandom.current().nextInt(array.length) + 5;
		for (int i = 0; i < array.length; i += bufSize) {
			int min = min(bufSize, buf.readRemaining());
			list.add(buf.slice(min));
			buf.moveHead(min);
		}
		buf.recycle();
		return list;
	}
}
