package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.csp.supplier.ChannelSuppliers;
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
		AsyncServlet servlet = request -> request.loadBody(Integer.MAX_VALUE).map(body -> HttpResponse.Builder.ok200()
				.withBody(body.slice())
				.build());

		HttpRequest testRequest = HttpRequest.Builder.post("http://example.com")
				.withBodyStream(ChannelSuppliers.ofValues(
						ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
						ByteBuf.wrapForReading("Test2".getBytes(UTF_8)))
				)
				.build();

		HttpResponse response = await(servlet.serveAsync(testRequest));
		testRequest.recycle();
		ByteBuf body = await(response.loadBody(Integer.MAX_VALUE));

		assertEquals("Test1Test2", body.asString(UTF_8));
	}

	@Test
	public void testEnsureRequestBodyWithException() throws Exception {
		AsyncServlet servlet = request -> request.loadBody(Integer.MAX_VALUE)
				.map(body -> HttpResponse.Builder.ok200().withBody(body.slice()).build());
		Exception exception = new Exception("TestException");

		ByteBuf byteBuf = ByteBufPool.allocate(100);
		byteBuf.put("Test1".getBytes(UTF_8));

		HttpRequest testRequest = HttpRequest.Builder.post("http://example.com")
				.withBodyStream(ChannelSuppliers.concat(
						ChannelSuppliers.ofValue(byteBuf),
						ChannelSuppliers.ofException(exception)
				))
				.build();

		Exception e = awaitException(servlet.serveAsync(testRequest));

		assertSame(exception, e);
	}
}
