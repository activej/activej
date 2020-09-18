package io.activej.http;

import io.activej.dns.CachedAsyncDnsClient;
import io.activej.dns.RemoteAsyncDnsClient;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.DatagramSocketSettings;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.http.TestUtils.readFully;
import static io.activej.http.TestUtils.toByteArray;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class SimpleProxyServerTest {
	private static final int ECHO_SERVER_PORT = getFreePort();
	private static final int PROXY_SERVER_PORT = getFreePort();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private void readAndAssert(InputStream is, String expected) {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		String actual = decodeAscii(bytes);
		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(actual.split("\r\n"))));
	}

	@Test
	public void testSimpleProxyServer() throws Exception {
		Eventloop eventloop1 = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer echoServer = AsyncHttpServer.create(eventloop1,
				request -> HttpResponse.ok200()
						.withBody(encodeAscii(request.getUrl().getPathAndQuery())))
				.withListenPort(ECHO_SERVER_PORT);
		echoServer.listen();

		Thread echoServerThread = new Thread(eventloop1);
		echoServerThread.start();

		Eventloop eventloop2 = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop2)
				.withDnsClient(CachedAsyncDnsClient.create(eventloop2, RemoteAsyncDnsClient.create(eventloop2)
						.withDatagramSocketSetting(DatagramSocketSettings.create())
						.withDnsServerAddress(HttpUtils.inetAddress("8.8.8.8"))));

		AsyncHttpServer proxyServer = AsyncHttpServer.create(eventloop2,
				request -> {
					String path = ECHO_SERVER_PORT + request.getUrl().getPath();
					return httpClient.request(HttpRequest.get("http://127.0.0.1:" + path))
							.then(result -> result.loadBody()
									.then(body -> Promise.of(HttpResponse.ofCode(result.getCode())
											.withBody(encodeAscii("FORWARDED: " + body
													.getString(UTF_8))))));
				})
				.withListenPort(PROXY_SERVER_PORT);
		proxyServer.listen();

		Thread proxyServerThread = new Thread(eventloop2);
		proxyServerThread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", PROXY_SERVER_PORT));
		OutputStream stream = socket.getOutputStream();

		stream.write(encodeAscii("GET /abc HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 15\r\n\r\nFORWARDED: /abc");
		stream.write(encodeAscii("GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 17\r\n\r\nFORWARDED: /hello");

		httpClient.getEventloop().execute(httpClient::stop);

		echoServer.closeFuture().get();
		proxyServer.closeFuture().get();

		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		echoServerThread.join();
		proxyServerThread.join();
	}
}
