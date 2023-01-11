package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.http.TestUtils.assertEmpty;
import static io.activej.http.TestUtils.readFully;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertingFn;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class HttpTolerantApplicationTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTolerantServer() throws Exception {
		int port = getFreePort();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrow());

		HttpServer server = HttpServer.create(eventloop,
						request ->
								Promise.ofCallback(cb ->
										eventloop.post(() -> cb.set(
												HttpResponse.ok200()
														.withBody(encodeAscii(request.getUrl().getPathAndQuery()))))))
				.withListenPort(port);

		server.listen();

		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		write(socket, """
				GET /abc  HTTP/1.1
				Host: \tlocalhost

				""");
		readAndAssert(socket.getInputStream(), """
				HTTP/1.1 200 OK\r
				Connection: keep-alive\r
				Content-Length: 4\r
				\r
				/abc""");
		write(socket, """
				GET /abc  HTTP/1.0
				Cost: \tlocalhost \t\s
				Connection: keep-alive

				""");
		readAndAssert(socket.getInputStream(), """
				HTTP/1.1 200 OK\r
				Connection: keep-alive\r
				Content-Length: 4\r
				\r
				/abc""");
		write(socket, """
				GET /abc  HTTP/1.0
				Cost: \tlocalhost \t\s

				""");
		readAndAssert(socket.getInputStream(), """
				HTTP/1.1 200 OK\r
				Connection: close\r
				Content-Length: 4\r
				\r
				/abc""");
		assertEmpty(socket.getInputStream());
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testTolerantClient() throws Exception {
		int port = getFreePort();

		ServerSocket listener = new ServerSocket(port);
		String text = "/abc";
		new Thread(() -> {
			while (Thread.currentThread().isAlive()) {
				try (Socket socket = listener.accept()) {
					System.out.println("accept: " + socket);
					DataInputStream in = new DataInputStream(socket.getInputStream());
					int b = 0;
					//noinspection StatementWithEmptyBody
					while (b != -1 && !(((b = in.read()) == CR || b == LF) && (b = in.read()) == LF)) ;
					System.out.println("write: " + socket);
					write(socket, "HTTP/1.1 200 OK\nContent-Type:  \t  text/html; charset=UTF-8\nContent-Length:  4\n\n" + text);
				} catch (IOException ignored) {
				}
			}
		})
				.start();

		String header = await(HttpClient_Reactive.create(Reactor.getCurrentReactor())
				.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody()
						.whenResult(body -> assertEquals(text, body.getString(UTF_8)))
						.map($ -> response.getHeader(HttpHeaders.CONTENT_TYPE))
						.whenComplete(assertingFn(($, e) -> {
							listener.close();
						}))));

		assertEquals("text/html; charset=UTF-8", header);
	}

	private static void write(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		socket.getOutputStream().write(buf.array(), buf.head(), buf.readRemaining());
	}

	private static void readAndAssert(InputStream is, String expected) {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		String actual = decodeAscii(bytes);
		assertEquals(new LinkedHashSet<>(List.of(expected.split("\r\n"))), new LinkedHashSet<>(List.of(actual.split("\r\n"))));
	}
}
