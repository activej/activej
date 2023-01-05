package io.activej.http;

import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class HttpServerClientBreakConnectionTest {
	private final Logger logger = LoggerFactory.getLogger(HttpServerClientBreakConnectionTest.class);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule bufRule = new ByteBufRule();

	private final NioReactor reactor = Reactor.getCurrentReactor();
	private HttpServer server;
	private AsyncHttpClient client;
	private int freePort;

	@Before
	public void init() throws IOException {
		freePort = getFreePort();
		server = HttpServer.create(reactor,
				request -> {
					logger.info("Closing server...");
					reactor.post(() ->
							server.close().whenComplete(() -> logger.info("Server Closed")));
					return Promises.delay(100L,
							HttpResponse.ok200()
									.withBody("Hello World".getBytes())
					);
				})
				.withListenPort(freePort)
				.withAcceptOnce();

		client = HttpClient.create(reactor);
		server.listen();
	}

	@Test
	public void testBreakConnection() {
		String result = await(client.request(
						HttpRequest.post("http://127.0.0.1:" + freePort)
								.withBody("Hello World".getBytes()))
				.then(response ->
						response.loadBody()
								.map(body -> body.getString(UTF_8))));

		assertEquals("Hello World", result);
	}
}
