package io.activej.http;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.activej.common.Utils.first;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class AsyncHttpServerClientBreakConnectionTest {
	private final Logger logger = LoggerFactory.getLogger(AsyncHttpServerClientBreakConnectionTest.class);

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
				.withListenPort(0)
				.withAcceptOnce();

		client = AsyncHttpClient.create(eventloop);
		server.listen();
	}

	@Test
	public void testBreakConnection() {
		String result = await(client.request(
						HttpRequest.post("http://127.0.0.1:" + first(server.getBoundAddresses()).getPort())
								.withBody("Hello World".getBytes()))
				.then(response ->
						response.loadBody()
								.map(body -> body.getString(UTF_8))));

		assertEquals("Hello World", result);
	}
}
