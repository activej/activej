package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
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

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.http.TestUtils.readFully;
import static io.activej.http.TestUtils.toByteArray;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.asserting;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class HttpTolerantApplicationTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTolerantServer() throws Exception {
		int port = getFreePort();

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
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
		write(socket, "GET /abc  HTTP/1.1\nHost: \tlocalhost\n\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		write(socket, "GET /abc  HTTP/1.0\nHost: \tlocalhost \t \nConnection: keep-alive\n\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		write(socket, "GET /abc  HTTP/1.0\nHost: \tlocalhost \t \n\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc");
		assertEquals(0, toByteArray(socket.getInputStream()).length);
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

		String header = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.get("http://127.0.0.1:" + port))
				.then(response -> response.loadBody()
						.whenResult(body -> assertEquals(text, body.getString(UTF_8)))
						.map($ -> response.getHeader(HttpHeaders.CONTENT_TYPE))
						.whenComplete(asserting(($, e) -> {
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
		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(actual.split("\r\n"))));
	}
}
