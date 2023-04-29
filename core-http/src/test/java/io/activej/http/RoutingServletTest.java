package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.TreeMap;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.http.HttpMethod.*;
import static io.activej.http.WebSocketConstants.NOT_A_WEB_SOCKET_REQUEST;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.test.TestUtils.assertCompleteFn;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public final class RoutingServletTest {
	private static final String TEMPLATE = "http://www.site.org";
	private static final String TEMPLATE_WS = "ws://www.site.org";
	private static final String DELIM = "*****************************************************************************";

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private void check(Promise<HttpResponse> promise, String expectedBody, int expectedCode) {
		assertTrue(promise.isComplete());
		if (promise.isResult()) {
			HttpResponse result = promise.getResult();
			result.loadBody()
					.whenComplete(assertCompleteFn($ -> {
						assertEquals(expectedBody, result.getBody().asString(UTF_8));
						assertEquals(expectedCode, result.getCode());
					}));
		} else {
			assertEquals(expectedCode, ((HttpError) promise.getException()).getCode());
		}
	}

	private void checkWebSocket(Promise<HttpResponse> promise) {
		assertTrue(promise.isException());
		assertSame(NOT_A_WEB_SOCKET_REQUEST, promise.getException());
	}

	@Test
	public void testBase() throws Exception {
		Reactor reactor = getCurrentReactor();
		RoutingServlet servlet1 = RoutingServlet.create(reactor);

		AsyncServlet subservlet = request -> HttpResponse.builder(200)
				.withBody("".getBytes(UTF_8))
				.toPromise();

		servlet1.map(GET, "/a/b/c", subservlet);

		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c")), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c/d")), "", 404);
		check(servlet1.serve(HttpRequest.post("http://some-test.com/a/b/c")), "", 404);

		RoutingServlet servlet2 = RoutingServlet.create(reactor);
		servlet2.map(HEAD, "/a/b/c", subservlet);

		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c")), "", 404);
		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c/d")), "", 404);
		check(servlet2.serve(HttpRequest.of(HEAD, "http://some-test.com/a/b/c")), "", 200);
	}

	@Test
	public void testProcessWildCardRequest() throws Exception {
		RoutingServlet servlet = RoutingServlet.create(getCurrentReactor());
		servlet.map("/a/b/c/d", request -> HttpResponse.builder(200)
				.withBody("".getBytes(UTF_8))
				.toPromise());

		check(servlet.serve(HttpRequest.get("http://some-test.com/a/b/c/d")), "", 200);
		check(servlet.serve(HttpRequest.post("http://some-test.com/a/b/c/d")), "", 200);
		check(servlet.serve(HttpRequest.of(OPTIONS, "http://some-test.com/a/b/c/d")), "", 200);
	}

	@Test
	public void testMicroMapping() throws Exception {
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
			return HttpResponse.builder(200).withBody(msg).toPromise();
		};

		Reactor reactor = getCurrentReactor();
		RoutingServlet a = RoutingServlet.create(reactor)
				.map(GET, "/c", action)
				.map(GET, "/d", action)
				.map(GET, "/", action);

		RoutingServlet b = RoutingServlet.create(reactor)
				.map(GET, "/f", action)
				.map(GET, "/g", action);

		RoutingServlet main = RoutingServlet.create(reactor)
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
	public void testLongMapping() throws Exception {
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
			return HttpResponse.builder(200).withBody(msg).toPromise();
		};

		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
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
	public void testMerge() throws Exception {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/");         // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a");        // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/b");        // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/c");      // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/d");      // ok
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/a/e");      // ok
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/a/c/f");    // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.builder(200).withBody(msg).toPromise();
		};

		Reactor reactor = getCurrentReactor();
		RoutingServlet main = RoutingServlet.create(reactor)
				.map(GET, "/a", action)
				.map(GET, "/a/c", action)
				.map(GET, "/a/d", action)
				.map(GET, "/b", action)
				.merge(RoutingServlet.create(reactor)
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
	public void testFailMerge() throws Exception {
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f");    // fail

		AsyncServlet action = req -> {
			ByteBuf msg = wrapUtf8("Executed: " + req.getPath());
			return HttpResponse.builder(200).withBody(msg).toPromise();
		};

		AsyncServlet anotherAction = req -> {
			ByteBuf msg = wrapUtf8("Shall not be executed: " + req.getPath());
			return HttpResponse.builder(200).withBody(msg).toPromise();
		};

		RoutingServlet main;
		try {
			Reactor reactor = getCurrentReactor();
			main = RoutingServlet.create(reactor)
					.map(GET, "/", action)
					.map(GET, "/a/e", action)
					.map(GET, "/a/c/f", action)
					.map(GET, "/", RoutingServlet.create(reactor)
							.map(GET, "/a/c/f", anotherAction));
		} catch (IllegalArgumentException e) {
			assertEquals("Already mapped", e.getMessage());
			return;
		}

		check(main.serve(request), "SHALL NOT BE EXECUTED", 500);
	}

	@Test
	public void testParameter() throws Exception {
		AsyncServlet printParameters = request -> {
			String body = request.getPathParameter("id")
					+ " " + request.getPathParameter("uid");
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.builder(200).withBody(bodyByteBuf).toPromise();
		};

		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/:id/a/:uid/b/:eid", printParameters)
				.map(GET, "/:id/a/:uid", printParameters);

		System.out.println("Parameter test " + DELIM);
		check(main.serve(HttpRequest.get("http://example.com/123/a/456/b/789")), "123 456", 200);
		check(main.serve(HttpRequest.get("http://example.com/555/a/777")), "555 777", 200);
		check(main.serve(HttpRequest.get("http://example.com")), "", 404);
		System.out.println();
	}

	@Test
	public void testMultiParameters() throws Exception {
		RoutingServlet ms = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/serve/:cid/wash", request -> {
					ByteBuf body = wrapUtf8("served car: " + request.getPathParameter("cid"));
					return HttpResponse.builder(200).withBody(body).toPromise();
				})
				.map(GET, "/serve/:mid/feed", request -> {
					ByteBuf body = wrapUtf8("served man: " + request.getPathParameter("mid"));
					return HttpResponse.builder(200).withBody(body).toPromise();
				});

		System.out.println("Multi parameters " + DELIM);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/1/wash")), "served car: 1", 200);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/2/feed")), "served man: 2", 200);
		System.out.println();
	}

	@Test
	public void testDifferentMethods() throws Exception {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action");
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action");
		HttpRequest request3 = HttpRequest.of(CONNECT, TEMPLATE + "/a/b/c/action");

		RoutingServlet servlet = RoutingServlet.create(getCurrentReactor())
				.map("/a/b/c/action", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("WILDCARD")).toPromise())
				.map(POST, "/a/b/c/action", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("POST")).toPromise())
				.map(GET, "/a/b/c/action", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("GET")).toPromise());

		System.out.println("Different methods " + DELIM);
		check(servlet.serve(request1), "GET", 200);
		check(servlet.serve(request2), "POST", 200);
		check(servlet.serve(request3), "WILDCARD", 200);
		System.out.println();
	}

	@Test
	public void testDefault() throws Exception {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action");
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban");

		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/html/admin/action", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("Action executed")).toPromise())
				.map("/html/admin/*", request ->
						HttpResponse.builder(200)
								.withBody(wrapUtf8("Stopped at admin: " + request.getRelativePath()))
								.toPromise());

		System.out.println("Default stop " + DELIM);
		check(main.serve(request1), "Action executed", 200);
		check(main.serve(request2), "Stopped at admin: action/ban", 200);
		System.out.println();
	}

	@Test
	public void test404() throws Exception {
		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map("/a/:id/b/d", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("All OK")).toPromise());

		System.out.println("404 " + DELIM);
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/123/b/c");
		check(main.serve(request), "", 404);
		System.out.println();
	}

	@Test
	public void test405() throws Exception {
		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/a/:id/b/d", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("Should not execute")).toPromise());

		HttpRequest request = HttpRequest.post(TEMPLATE + "/a/123/b/d");
		check(main.serve(request), "", 404);
	}

	@Test
	public void test405WithFallback() throws Exception {
		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/a/:id/b/d", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("Should not execute")).toPromise())
				.map("/a/:id/b/d", request ->
						HttpResponse.builder(200).withBody(wrapUtf8("Fallback executed")).toPromise());
		check(main.serve(HttpRequest.post(TEMPLATE + "/a/123/b/d")), "Fallback executed", 200);
	}

	@Test
	public void testTail() throws Exception {
		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/method/:var/*", request -> {
					ByteBuf body = wrapUtf8("Success: " + request.getRelativePath());
					return HttpResponse.builder(200).withBody(body).toPromise();
				});

		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dfbdb/oneArg")), "Success: oneArg", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/srfethj/first/second")), "Success: first/second", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dvyhju/")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn?query=string")), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn/first?query=string")), "Success: first", 200);
	}

	@Test
	public void testWebSocket() throws Exception {
		String wsPath = "/web/socket";
		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.mapWebSocket(wsPath, request -> HttpResponse.ok200())
				.map(POST, wsPath, request -> HttpResponse.Builder.ok200().withBody(wrapUtf8("post")).toPromise())
				.map(GET, wsPath, request -> HttpResponse.Builder.ok200().withBody(wrapUtf8("get")).toPromise())
				.map(wsPath, request -> HttpResponse.Builder.ok200().withBody(wrapUtf8("all")).toPromise())
				.map(wsPath + "/inside", request -> HttpResponse.Builder.ok200().withBody(wrapUtf8("inner")).toPromise());

		checkWebSocket(main.serve(HttpRequest.get(TEMPLATE_WS + wsPath)));
		check(main.serve(HttpRequest.post(TEMPLATE + wsPath)), "post", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + wsPath)), "get", 200);
		check(main.serve(HttpRequest.of(OPTIONS, TEMPLATE + wsPath)), "all", 200);
		check(main.serve(HttpRequest.of(OPTIONS, TEMPLATE + wsPath + "/inside")), "inner", 200);
	}

	@Test
	public void testWebSocketSingle() throws Exception {
		String wsPath = "/web/socket";
		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.mapWebSocket(wsPath, request -> HttpResponse.ok200());

		checkWebSocket(main.serve(HttpRequest.get(TEMPLATE_WS + wsPath)));
		check(main.serve(HttpRequest.get(TEMPLATE + wsPath)), "", 404);
		check(main.serve(HttpRequest.post(TEMPLATE + wsPath)), "", 404);
	}

	@Test
	public void testPercentEncoding() throws Exception {
		RoutingServlet router = RoutingServlet.create(getCurrentReactor());

		AsyncServlet servlet = request -> HttpResponse.builder(200).withBody("".getBytes(UTF_8)).toPromise();

		router.map(GET, "/a%2fb", servlet);

		try {
			router.map(GET, "/a%2Fb", servlet);
			fail();
		} catch (IllegalArgumentException e) {
			assertEquals("Already mapped", e.getMessage());
		}

		check(router.serve(HttpRequest.get("http://some-test.com/a%2fb")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/a%2Fb")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/a/b")), "", 404);
	}

	@Test
	public void testPercentEncodingCyrillic() throws Exception {
		RoutingServlet router = RoutingServlet.create(getCurrentReactor());

		AsyncServlet servlet = request -> HttpResponse.builder(200).withBody("".getBytes(UTF_8)).toPromise();

		router.map(GET, "/абв", servlet);

		try {
			router.map(GET, "/%D0%B0%D0%B1%D0%B2", servlet);
			fail();
		} catch (IllegalArgumentException e) {
			assertEquals("Already mapped", e.getMessage());
		}

		check(router.serve(HttpRequest.get("http://some-test.com/%d0%b0%d0%b1%d0%b2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%b0%d0%b1%d0%b2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%d0%b1%d0%b2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%b1%d0%b2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%B1%d0%b2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%B1%D0%b2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%B1%D0%B2")), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/абв")), "", 404);
	}

	@Test
	public void testPercentEncodedParameters() throws Exception {
		AsyncServlet printParameters = request -> {
			String body = new TreeMap<>(request.getPathParameters()).toString();
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.builder(200).withBody(bodyByteBuf).toPromise();
		};

		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/a/:val", printParameters);

		check(main.serve(HttpRequest.get("http://example.com/a/1%2f")), "{val=1/}", 200);
		check(main.serve(HttpRequest.get("http://example.com/a/1%2F")), "{val=1/}", 200);
		check(main.serve(HttpRequest.get("http://example.com/a/1+")), "{val=1 }", 200);
		check(main.serve(HttpRequest.get("http://example.com/a/1%252f")), "{val=1%2f}", 200);
	}

	@Test
	public void testPercentEncodedParameterName() throws Exception {
		AsyncServlet printParameters = request -> {
			String body = new TreeMap<>(request.getPathParameters()).toString();
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.builder(200).withBody(bodyByteBuf).toPromise();
		};

		RoutingServlet main = RoutingServlet.create(getCurrentReactor())
				.map(GET, "/a/:%2f", printParameters);

		check(main.serve(HttpRequest.get("http://example.com/a/23")), "{%2f=23}", 200);
	}

	@Test
	public void testBadPercentEncoding() throws Exception {
		RoutingServlet router = RoutingServlet.create(getCurrentReactor());
		AsyncServlet servlet = request -> HttpResponse.builder(200).withBody(new byte[0]).toPromise();
		RoutingServlet main = router.map(GET, "/a", servlet);

		try {
			main.serve(HttpRequest.get("http://example.com/a%2"));
			fail();
		} catch (HttpError e) {
			assertEquals("HTTP code 400: Path contains bad percent encoding", e.getMessage());
		}

		try {
			router.map(GET, "/a%2", servlet);
			fail();
		} catch (IllegalArgumentException e) {
			assertEquals("Pattern contains bad percent encoding", e.getMessage());
		}
	}
}
