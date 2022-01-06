package io.activej.http;

import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static io.activej.common.Utils.first;
import static io.activej.http.HttpHeaders.ALLOW;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public final class TestClientMultilineHeaders {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testMultilineHeaders() throws IOException {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
						request -> {
							HttpResponse response = HttpResponse.ok200();
							response.addHeader(ALLOW, "GET,\r\n HEAD");
							return response;
						})
				.withListenPort(0)
				.withAcceptOnce();
		server.listen();

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
		String allowHeader = await(client.request(HttpRequest.get("http://127.0.0.1:" + first(server.getBoundAddresses()).getPort()))
				.map(response -> response.getHeader(ALLOW)));

		assertEquals("GET,   HEAD", allowHeader);
	}
}
