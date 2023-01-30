package io.activej.http;

import io.activej.dns.DnsClient;
import io.activej.dns.CachedDnsClient;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.test.rules.ByteBufRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.http.TestUtils.assertEmpty;
import static io.activej.http.TestUtils.readFully;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class SimpleProxyServerTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private int echoServerPort;
	private int proxyServerPort;

	@Before
	public void setUp() {
		echoServerPort = getFreePort();
		proxyServerPort = getFreePort();
	}

	private void readAndAssert(InputStream is, String expected) {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		String actual = decodeAscii(bytes);
		assertEquals(new LinkedHashSet<>(List.of(expected.split("\r\n"))), new LinkedHashSet<>(List.of(actual.split("\r\n"))));
	}

	@Test
	public void testSimpleProxyServer() throws Exception {
		Eventloop eventloop1 = Eventloop
				.builder()
				.withFatalErrorHandler(rethrow())
				.withCurrentThread()
				.build();

		HttpServer echoServer = HttpServer.builder(eventloop1,
						request -> HttpResponse.ok200()
								.withBody(encodeAscii(request.getUrl().getPathAndQuery())))
				.withListenPort(echoServerPort)
				.build();
		echoServer.listen();

		Thread echoServerThread = new Thread(eventloop1);
		echoServerThread.start();

		Eventloop eventloop2 = Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.withCurrentThread()
				.build();

		IHttpClient httpClient = HttpClient.builder(eventloop2)
				.withDnsClient(CachedDnsClient.create(eventloop2, DnsClient.builder(eventloop2)
						.withDatagramSocketSetting(DatagramSocketSettings.create())
						.withDnsServerAddress(HttpUtils.inetAddress("8.8.8.8"))
						.build()))
				.build();

		HttpServer proxyServer = HttpServer.builder(eventloop2,
						request -> {
							String path = echoServerPort + request.getUrl().getPath();
							return httpClient.request(HttpRequest.get("http://127.0.0.1:" + path))
									.then(result -> result.loadBody()
											.then(body -> Promise.of(HttpResponse.ofCode(result.getCode())
													.withBody(encodeAscii("FORWARDED: " + body
															.getString(UTF_8))))));
						})
				.withListenPort(proxyServerPort)
				.build();
		proxyServer.listen();

		Thread proxyServerThread = new Thread(eventloop2);
		proxyServerThread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", proxyServerPort));
		OutputStream stream = socket.getOutputStream();

		stream.write(encodeAscii("""
				GET /abc HTTP/1.1\r
				Host: localhost\r
				Connection: keep-alive
				\r
				"""));
		readAndAssert(socket.getInputStream(), """
				HTTP/1.1 200 OK\r
				Connection: keep-alive\r
				Content-Length: 15\r
				\r
				FORWARDED: /abc""");
		stream.write(encodeAscii("""
				GET /hello HTTP/1.1\r
				Host: localhost\r
				Connection: close
				\r
				"""));
		readAndAssert(socket.getInputStream(), """
				HTTP/1.1 200 OK\r
				Connection: close\r
				Content-Length: 17\r
				\r
				FORWARDED: /hello""");

		echoServer.closeFuture().get();
		proxyServer.closeFuture().get();

		assertEmpty(socket.getInputStream());
		socket.close();

		echoServerThread.join();
		proxyServerThread.join();
	}
}
