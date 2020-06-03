package io.activej.http;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.test.TestUtils;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AsyncHttpServerClientBreakConnectionTest {
	private final Logger logger = LoggerFactory.getLogger(AsyncHttpServerClientBreakConnectionTest.class);
	private final int FREE_PORT = TestUtils.getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule bufRule = new ByteBufRule();

	private final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private AsyncHttpServer server;
	private AsyncHttpClient client;

	@Before
	public void init() throws IOException {
		server = AsyncHttpServer.create(eventloop,
				request -> {
					logger.info("Closing server...");
					eventloop.post(() ->
							server.close().whenComplete(() -> logger.info("Server Closed")));
					return Promises.delay(100L,
							HttpResponse.ok200()
									.withBody("Hello World".getBytes())
					);
				})
				.withListenPort(FREE_PORT)
				.withAcceptOnce();

		client = AsyncHttpClient.create(eventloop);
		server.listen();
	}

	@Test
	public void testBreakConnection() {
		await(client.request(
				HttpRequest.post("http://127.0.0.1:" + FREE_PORT)
						.withBody("Hello World".getBytes()))
				.map(response ->
						response.loadBody()
								.map(body -> body.getString(UTF_8))));
	}
}
