package io.activej.launchers.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.config.Config;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.net.PrimaryServer;
import io.activej.service.ServiceGraph;
import io.activej.test.rules.ByteBufRule;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class HttpWorkerServerTest {
	public static final int PORT = getFreePort();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws Exception {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Provides
			@Worker
			AsyncServlet servlet(@WorkerId int worker) {
				return request ->
						HttpResponse.ok200()
								.withBody(ByteBuf.wrapForReading(encodeAscii("Hello, world! #" + worker)));
			}

			@Override
			protected Module getOverrideModule() {
				return new AbstractModule() {
					@Provides
					Config config() {
						return Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(HttpWorkerServerTest.PORT)));
					}
				};
			}
		};
		Injector injector = launcher.createInjector(new String[]{});
		injector.getInstance(PrimaryServer.class);

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try (Socket socket0 = new Socket(); Socket socket1 = new Socket()) {
			serviceGraph.startFuture().get();

			InetSocketAddress localhost = new InetSocketAddress("localhost", PORT);
			socket0.connect(localhost);
			socket1.connect(localhost);

			for (int i = 0; i < 10; i++) {
				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #0");

				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #0");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #1");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #1");
			}
		} finally {
			serviceGraph.stopFuture().get();
		}
	}

	private static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];

		int length = bytes.length;
		int total = 0;
		while (total < length) {
			int result = is.read(bytes, total, length - total);
			if (result == -1) {
				break;
			}
			total += result;
		}

		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(decodeAscii(bytes).split("\r\n"))));
	}
}

