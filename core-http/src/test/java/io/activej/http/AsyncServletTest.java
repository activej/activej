package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AsyncServletTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testEnsureRequestBody() throws Exception {
		AsyncServlet servlet = request -> request.loadBody(Integer.MAX_VALUE).map(body -> HttpResponse.ok200().withBody(body.slice()));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSupplier.of(
						ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
						ByteBuf.wrapForReading("Test2".getBytes(UTF_8)))
				);

		HttpResponse response = await(servlet.serveAsync(testRequest));
		testRequest.recycle();
		ByteBuf body = await(response.loadBody(Integer.MAX_VALUE));

		assertEquals("Test1Test2", body.asString(UTF_8));
	}

	@Test
	public void testEnsureRequestBodyWithException() throws Exception {
		AsyncServlet servlet = request -> request.loadBody(Integer.MAX_VALUE)
				.map(body -> HttpResponse.ok200().withBody(body.slice()));
		Exception exception = new Exception("TestException");

		ByteBuf byteBuf = ByteBufPool.allocate(100);
		byteBuf.put("Test1".getBytes(UTF_8));

		HttpRequest testRequest = HttpRequest.post("http://example.com")
				.withBodyStream(ChannelSuppliers.concat(
						ChannelSupplier.of(byteBuf),
						ChannelSupplier.ofException(exception)
				));

		Throwable e = awaitException(servlet.serveAsync(testRequest));

		assertSame(exception, e);
	}
}
