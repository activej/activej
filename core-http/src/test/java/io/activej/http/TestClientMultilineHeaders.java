package io.activej.http;

import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static io.activej.http.HttpHeaders.ALLOW;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

public final class TestClientMultilineHeaders {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private int port;

	@Before
	public void setUp() {
		port = getFreePort();
	}

	@Test
	public void testMultilineHeaders() throws IOException {
		HttpServer.builder(Reactor.getCurrentReactor(),
				request -> HttpResponse.ok200()
					.withHeader(ALLOW, "GET,\r\n HEAD")
					.toPromise())
			.withListenPort(port)
			.withAcceptOnce()
			.build()
			.listen();

		IHttpClient client = HttpClient.create(Reactor.getCurrentReactor());
		String allowHeader = await(client.request(HttpRequest.get("http://127.0.0.1:" + port).build())
			.map(response -> response.getHeader(ALLOW)));

		assertEquals("GET,   HEAD", allowHeader);
	}
}
