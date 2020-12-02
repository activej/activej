package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
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

import static io.activej.http.stream.BufsConsumerChunkedDecoder.CRLF;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class HttpStreamTest {
	private static final int PORT = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private final String requestBody = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor.\n" +
			"Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.\n" +
			"Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim.\n" +
			"Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu.\n" +
			"In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium.";

	private List<ByteBuf> expectedList;

	@Before
	public void setUp() {
		expectedList = getBufsList(requestBody.getBytes());
	}

	@Test
	public void testStreamUpload() throws IOException {
		startTestServer(request -> request
				.getBodyStream()
				.async()
				.toCollector(ByteBufQueue.collector())
				.whenComplete(assertComplete(buf -> assertEquals(requestBody, buf.asString(UTF_8))))
				.then(s -> Promise.of(HttpResponse.ok200())));

		Integer code = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(ChannelSupplier.ofList(expectedList)
								.mapAsync(item -> Promises.delay(1L, item))))
				.async()
				.map(HttpResponse::getCode));

		assertEquals((Integer) 200, code);
	}

	@Test
	public void testStreamDownload() throws IOException {
		startTestServer(request ->
				HttpResponse.ok200()
						.withBodyStream(ChannelSupplier.ofList(expectedList)
								.mapAsync(item -> Promises.delay(1L, item))));

		ByteBuf body = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT))
				.async()
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())))
				.then(response -> response.getBodyStream().async().toCollector(ByteBufQueue.collector())));

		assertEquals(requestBody, body.asString(UTF_8));
	}

	@Test
	public void testLoopBack() throws IOException {
		startTestServer(request -> request
				.getBodyStream()
				.async()
				.toList()
				.map(ChannelSupplier::ofList)
				.then(bodyStream -> Promise.of(HttpResponse.ok200().withBodyStream(bodyStream.async()))));

		ByteBuf body = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(ChannelSupplier.ofList(expectedList)
								.mapAsync(item -> Promises.delay(1L, item))))
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())))
				.then(response -> response.getBodyStream().async().toCollector(ByteBufQueue.collector())));

		assertEquals(requestBody, body.asString(UTF_8));
	}

	@Test
	public void testCloseWithError() throws IOException {
		String exceptionMessage = "Test Exception";

		startTestServer(request -> Promise.ofException(new HttpError(432, exceptionMessage)));

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofList(expectedList);

		ByteBuf body = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(supplier))
				.then(response -> response.getBodyStream().toCollector(ByteBufQueue.collector())));

		assertTrue(body.asString(UTF_8).contains(exceptionMessage));
	}

	@Test
	public void testChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"4" + crlf + "Test" + crlf + "0" + crlf + crlf;

		String responseMessage =
				"HTTP/1.1 200 OK" + crlf +
						"Content-Length: 4" + crlf +
						"Connection: keep-alive" + crlf + crlf +
						"Test";

		ByteBuf body = await(AsyncTcpSocketNio.connect(new InetSocketAddress(PORT))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then(() -> socket.write(null))
						.then(() -> ChannelSupplier.ofSocket(socket).toCollector(ByteBufQueue.collector()))
						.whenComplete(socket::close)));

		assertEquals(responseMessage, body.asString(UTF_8));

		// not used here
		Recyclers.recycle(expectedList);
	}

	@Test
	public void testMalformedChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"ffffffffff";

		ByteBuf body = await(AsyncTcpSocketNio.connect(new InetSocketAddress(PORT))
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
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Content-Length: 13" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"3";

		ByteBuf body = await(AsyncTcpSocketNio.connect(new InetSocketAddress(PORT))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then(() -> socket.write(null))
						.then(socket::read)
						.whenComplete(socket::close)));

		assertNull(body);

//		String response = body.asString(UTF_8);
//		assertTrue(response.contains("HTTP/1.1 400 Bad Request"));
//		assertTrue(response.contains("Incomplete HTTP message"));

		// not used here
		Recyclers.recycle(expectedList);
	}

	@Test
	public void testSendingErrors() throws IOException {
		Exception exception = new Exception("Test Exception");

		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		Throwable e = awaitException(
				AsyncHttpClient.create(Eventloop.getCurrentEventloop())
						.request(HttpRequest.post("http://127.0.0.1:" + PORT)
								.withBodyStream(ChannelSuppliers.concat(
										ChannelSupplier.ofList(expectedList),
										ChannelSupplier.ofException(exception))))
						.then(response -> response.getBodyStream().toCollector(ByteBufQueue.collector())));

		assertThat(e, instanceOf(HttpException.class));
		assertSame(exception, e.getCause());
	}

	private void startTestServer(AsyncServlet servlet) throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(), servlet)
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();
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
