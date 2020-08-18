package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.http.HttpMethod.*;
import static io.activej.http.WebSocketConstants.NOT_A_WEB_SOCKET_REQUEST;
import static io.activej.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public final class RoutingServletTest {
	private static final String TEMPLATE = "http://www.site.org";
	private static final String TEMPLATE_WS = "ws://www.site.org";
	private static final String DELIM = "*****************************************************************************";

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private void check(Promise<HttpResponse> promise, String expectedBody, int expectedCode) {
		assertTrue(promise.isComplete());
		if (promise.isResult()) {
			HttpResponse result = promise.getResult();
			result.loadBody()
					.whenComplete(assertComplete($ -> {
						assertEquals(expectedBody, result.getBody().asString(UTF_8));
						assertEquals(expectedCode, result.getCode());
					}));
		} else {
			assertEquals(expectedCode, ((HttpException) promise.getException()).getCode());
		}
	}

	private void checkWebSocket(Promise<HttpResponse> promise) {
		assertTrue(promise.isException());
		assertSame(NOT_A_WEB_SOCKET_REQUEST, promise.getException());
	}

	@Test
	public void testBase() {
		RoutingServlet servlet1 = RoutingServlet.create();

		AsyncServlet subservlet = request -> HttpResponse.ofCode(200).withBody("".getBytes(UTF_8));

		servlet1.map(GET, "/a/b/c", subservlet);

		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c/d")), "", 404);
		check(servlet1.serve(HttpRequest.post("http://some-test.com/a/b/c")), "", 404);

		RoutingServlet servlet2 = RoutingServlet.create();
		servlet2.map(HEAD, "/a/b/c", subservlet);

		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c")), "", 404);
		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c/d")), "", 404);
		check(servlet2.serve(HttpRequest.of(HEAD, "http://some-test.com/a/b/c")), "", 200);
	}

	@Test
	public void testProcessWildCardRequest() {
		RoutingServlet servlet = RoutingServlet.create();
		servlet.map("/a/b/c/d", request -> HttpResponse.ofCode(200).withBody("".getBytes(UTF_8)));

		check(servlet.serve(HttpRequest.get("http://some-test.com/a/b/c/d")), "", 200);
		check(servlet.serve(HttpRequest.post("http://some-test.com/a/b/c/d")), "", 200);
		check(servlet.serve(HttpRequest.of(OPTIONS, "http://some-test.com/a/b/c/d")), "", 200);
	}

	@Test
	public void testMicroMapping() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c");  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d");  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e");  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b");    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f");  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g");  // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.ofCode(200).withBody(msg);
		};

		RoutingServlet a = RoutingServlet.create()
				.map(GET, "/c", action)
				.map(GET, "/d", action)
				.map(GET, "/", action);

		RoutingServlet b = RoutingServlet.create()
				.map(GET, "/f", action)
				.map(GET, "/g", action);

		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/", action)
				.map(GET, "/a/*", a)
				.map(GET, "/b/*", b);

		System.out.println("Micro mapping" + DELIM);
		check(main.serve(request1), "Executed: /", 200);
		check(main.serve(request2), "Executed: /a", 200);
		check(main.serve(request3), "Executed: /a/c", 200);
		check(main.serve(request4), "Executed: /a/d", 200);
		check(main.serve(request5), "", 404);
		check(main.serve(request6), "", 404);
		check(main.serve(request7), "Executed: /b/f", 200);
		check(main.serve(request8), "Executed: /b/g", 200);
		System.out.println();

		//		request5.recycleBufs();
		//		request6.recycleBufs();
	}

	@Test
	public void testLongMapping() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c");  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d");  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e");  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b");    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f");  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g");  // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.ofCode(200).withBody(msg);
		};

		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/", action)
				.map(GET, "/a", action)
				.map(GET, "/a/c", action)
				.map(GET, "/a/d", action)
				.map(GET, "/b/f", action)
				.map(GET, "/b/g", action);

		System.out.println("Long mapping " + DELIM);
		check(main.serve(request1), "Executed: /", 200);
		check(main.serve(request2), "Executed: /a", 200);
		check(main.serve(request3), "Executed: /a/c", 200);
		check(main.serve(request4), "Executed: /a/d", 200);
		check(main.serve(request5), "", 404);
		check(main.serve(request6), "", 404);
		check(main.serve(request7), "Executed: /b/f", 200);
		check(main.serve(request8), "Executed: /b/g", 200);
		System.out.println();

		//		request5.recycleBufs();
		//		request6.recycleBufs();
	}

	@Test
	public void testMerge() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");         // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");        // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/b");        // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/c");      // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/d");      // ok
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/a/e");      // ok
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/a/c/f");    // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.ofCode(200).withBody(msg);
		};

		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/a", action)
				.map(GET, "/a/c", action)
				.map(GET, "/a/d", action)
				.map(GET, "/b", action)
				.merge(RoutingServlet.create()
						.map(GET, "/", action)
						.map(GET, "/a/e", action)
						.map(GET, "/a/c/f", action));

		System.out.println("Merge   " + DELIM);
		check(main.serve(request1), "Executed: /", 200);
		check(main.serve(request2), "Executed: /a", 200);
		check(main.serve(request3), "Executed: /b", 200);
		check(main.serve(request4), "Executed: /a/c", 200);
		check(main.serve(request5), "Executed: /a/d", 200);
		check(main.serve(request6), "Executed: /a/e", 200);
		check(main.serve(request7), "Executed: /a/c/f", 200);
		System.out.println();
	}

	@Test
	public void testFailMerge() {
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f");    // fail

		AsyncServlet action = req -> {
			ByteBuf msg = wrapUtf8("Executed: " + req.getPath());
			return HttpResponse.ofCode(200).withBody(msg);
		};

		AsyncServlet anotherAction = req -> {
			ByteBuf msg = wrapUtf8("Shall not be executed: " + req.getPath());
			return HttpResponse.ofCode(200).withBody(msg);
		};

		RoutingServlet main;
		try {
			main = RoutingServlet.create()
					.map(GET, "/", action)
					.map(GET, "/a/e", action)
					.map(GET, "/a/c/f", action)
					.map(GET, "/", RoutingServlet.create()
							.map(GET, "/a/c/f", anotherAction));
		} catch (IllegalArgumentException e) {
			assertEquals("Already mapped", e.getMessage());
			return;
		}

		check(main.serve(request), "SHALL NOT BE EXECUTED", 500);
	}

	@Test
	public void testParameter() {
		AsyncServlet printParameters = request -> {
			String body = request.getPathParameter("id")
					+ " " + request.getPathParameter("uid");
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.ofCode(200).withBody(bodyByteBuf);
		};

		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/:id/a/:uid/b/:eid", printParameters)
				.map(GET, "/:id/a/:uid", printParameters);

		System.out.println("Parameter test " + DELIM);
		check(main.serve(HttpRequest.get("http://example.com/123/a/456/b/789")), "123 456", 200);
		check(main.serve(HttpRequest.get("http://example.com/555/a/777")), "555 777", 200);
		check(main.serve(HttpRequest.get("http://example.com")), "", 404);
		System.out.println();
	}

	@Test
	public void testMultiParameters() {
		RoutingServlet ms = RoutingServlet.create()
				.map(GET, "/serve/:cid/wash", request -> {
					ByteBuf body = wrapUtf8("served car: " + request.getPathParameter("cid"));
					return HttpResponse.ofCode(200).withBody(body);
				})
				.map(GET, "/serve/:mid/feed", request -> {
					ByteBuf body = wrapUtf8("served man: " + request.getPathParameter("mid"));
					return HttpResponse.ofCode(200).withBody(body);
				});

		System.out.println("Multi parameters " + DELIM);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/1/wash")), "served car: 1", 200);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/2/feed")), "served man: 2", 200);
		System.out.println();
	}

	@Test
	public void testDifferentMethods() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action");
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action");
		HttpRequest request3 = HttpRequest.of(CONNECT, TEMPLATE + "/a/b/c/action");

		RoutingServlet servlet = RoutingServlet.create()
				.map("/a/b/c/action", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("WILDCARD")))
				.map(POST, "/a/b/c/action", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("POST")))
				.map(GET, "/a/b/c/action", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("GET")));

		System.out.println("Different methods " + DELIM);
		check(servlet.serve(request1), "GET", 200);
		check(servlet.serve(request2), "POST", 200);
		check(servlet.serve(request3), "WILDCARD", 200);
		System.out.println();
	}

	@Test
	public void testDefault() {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action");
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban");

		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/html/admin/action", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("Action executed")))
				.map("/html/admin/*", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("Stopped at admin: " + request.getRelativePath())));

		System.out.println("Default stop " + DELIM);
		check(main.serve(request1), "Action executed", 200);
		check(main.serve(request2), "Stopped at admin: action/ban", 200);
		System.out.println();
	}

	@Test
	public void test404() {
		RoutingServlet main = RoutingServlet.create()
				.map("/a/:id/b/d", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("All OK")));

		System.out.println("404 " + DELIM);
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/123/b/c");
		check(main.serve(request), "", 404);
		System.out.println();
	}

	@Test
	public void test405() {
		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/a/:id/b/d", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("Should not execute")));

		HttpRequest request = HttpRequest.post(TEMPLATE + "/a/123/b/d");
		check(main.serve(request), "", 404);
	}

	@Test
	public void test405WithFallback() {
		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/a/:id/b/d", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("Should not execute")))
				.map("/a/:id/b/d", request ->
						HttpResponse.ofCode(200).withBody(wrapUtf8("Fallback executed")));
		check(main.serve(HttpRequest.post(TEMPLATE + "/a/123/b/d")), "Fallback executed", 200);
	}

	@Test
	public void testTail() {
		RoutingServlet main = RoutingServlet.create()
				.map(GET, "/method/:var/*", request -> {
					ByteBuf body = wrapUtf8("Success: " + request.getRelativePath());
					return HttpResponse.ofCode(200).withBody(body);
				});

		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dfbdb/oneArg")), "Success: oneArg", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/srfethj/first/second")), "Success: first/second", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dvyhju/")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn?query=string")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn/first?query=string")), "Success: first", 200);
	}

	@Test
	public void testWebSocket() {
		String wsPath = "/web/socket";
		RoutingServlet main = RoutingServlet.create()
				.mapWebSocket(wsPath, request -> HttpResponse.ok200())
				.map(POST, wsPath, request -> HttpResponse.ok200().withBody(wrapUtf8("post")))
				.map(GET, wsPath, request -> HttpResponse.ok200().withBody(wrapUtf8("get")))
				.map(wsPath, request -> HttpResponse.ok200().withBody(wrapUtf8("all")))
				.map(wsPath + "/inside", request -> HttpResponse.ok200().withBody(wrapUtf8("inner")));

		checkWebSocket(main.serve(HttpRequest.webSocket(TEMPLATE_WS + wsPath)));
		check(main.serve(HttpRequest.post(TEMPLATE + wsPath)), "post", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + wsPath)), "get", 200);
		check(main.serve(HttpRequest.of(OPTIONS, TEMPLATE + wsPath)), "all", 200);
		check(main.serve(HttpRequest.of(OPTIONS, TEMPLATE + wsPath + "/inside")), "inner", 200);
	}

	@Test
	public void testWebSocketSingle() {
		String wsPath = "/web/socket";
		RoutingServlet main = RoutingServlet.create()
				.mapWebSocket(wsPath, request -> HttpResponse.ok200());

		checkWebSocket(main.serve(HttpRequest.webSocket(TEMPLATE_WS + wsPath)));
		check(main.serve(HttpRequest.get(TEMPLATE + wsPath)), "", 404);
		check(main.serve(HttpRequest.post(TEMPLATE + wsPath)), "", 404);
	}

}
