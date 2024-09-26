package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
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
	public static final EventloopRule eventloopRule = new EventloopRule();

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

		AsyncServlet subservlet = request -> HttpResponse.ofCode(200)
			.withBody("".getBytes(UTF_8))
			.toPromise();

		RoutingServlet servlet1 = RoutingServlet.builder(reactor)
			.with(GET, "/a/b/c", subservlet)
			.build();

		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c").build()), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c").build()), "", 200);
		check(servlet1.serve(HttpRequest.get("http://some-test.com/a/b/c/d").build()), "", 404);
		check(servlet1.serve(HttpRequest.post("http://some-test.com/a/b/c").build()), "", 404);

		RoutingServlet servlet2 = RoutingServlet.builder(reactor)
			.with(HEAD, "/a/b/c", subservlet)
			.build();

		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c").build()), "", 404);
		check(servlet2.serve(HttpRequest.post("http://some-test.com/a/b/c/d").build()), "", 404);
		check(servlet2.serve(HttpRequest.builder(HEAD, "http://some-test.com/a/b/c").build()), "", 200);
	}

	@Test
	public void testProcessWildCardRequest() throws Exception {
		RoutingServlet servlet = RoutingServlet.builder(getCurrentReactor())
			.with("/a/b/c/d", request -> HttpResponse.ofCode(200)
				.withBody("".getBytes(UTF_8))
				.toPromise())
			.build();

		check(servlet.serve(HttpRequest.get("http://some-test.com/a/b/c/d").build()), "", 200);
		check(servlet.serve(HttpRequest.post("http://some-test.com/a/b/c/d").build()), "", 200);
		check(servlet.serve(HttpRequest.builder(OPTIONS, "http://some-test.com/a/b/c/d").build()), "", 200);
	}

	@Test
	public void testMicroMapping() throws Exception {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/").build();     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a").build();    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c").build();  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d").build();  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e").build();  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b").build();    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f").build();  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g").build();  // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.ofCode(200).withBody(msg).toPromise();
		};

		Reactor reactor = getCurrentReactor();
		RoutingServlet a = RoutingServlet.builder(reactor)
			.with(GET, "/c", action)
			.with(GET, "/d", action)
			.with(GET, "/", action)
			.build();

		RoutingServlet b = RoutingServlet.builder(reactor)
			.with(GET, "/f", action)
			.with(GET, "/g", action)
			.build();

		RoutingServlet main = RoutingServlet.builder(reactor)
			.with(GET, "/", action)
			.with(GET, "/a/*", a)
			.with(GET, "/b/*", b)
			.build();

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
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/").build();     // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a").build();    // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/a/c").build();  // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/d").build();  // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/e").build();  // 404
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/b").build();    // 404
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/b/f").build();  // ok
		HttpRequest request8 = HttpRequest.get(TEMPLATE + "/b/g").build();  // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.ofCode(200).withBody(msg).toPromise();
		};

		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/", action)
			.with(GET, "/a", action)
			.with(GET, "/a/c", action)
			.with(GET, "/a/d", action)
			.with(GET, "/b/f", action)
			.with(GET, "/b/g", action)
			.build();

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
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/").build();         // ok
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/a").build();        // ok
		HttpRequest request3 = HttpRequest.get(TEMPLATE + "/b").build();        // ok
		HttpRequest request4 = HttpRequest.get(TEMPLATE + "/a/c").build();      // ok
		HttpRequest request5 = HttpRequest.get(TEMPLATE + "/a/d").build();      // ok
		HttpRequest request6 = HttpRequest.get(TEMPLATE + "/a/e").build();      // ok
		HttpRequest request7 = HttpRequest.get(TEMPLATE + "/a/c/f").build();    // ok

		AsyncServlet action = request -> {
			ByteBuf msg = wrapUtf8("Executed: " + request.getPath());
			return HttpResponse.ofCode(200).withBody(msg).toPromise();
		};

		Reactor reactor = getCurrentReactor();
		RoutingServlet main = RoutingServlet.builder(reactor)
			.with(GET, "/a", action)
			.with(GET, "/a/c", action)
			.with(GET, "/a/d", action)
			.with(GET, "/b", action)
			.merge(RoutingServlet.builder(reactor)
				.with(GET, "/", action)
				.with(GET, "/a/e", action)
				.with(GET, "/a/c/f", action)
				.build())
			.build();

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
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/c/f").build();    // fail

		AsyncServlet action = req -> {
			ByteBuf msg = wrapUtf8("Executed: " + req.getPath());
			return HttpResponse.ofCode(200).withBody(msg).toPromise();
		};

		AsyncServlet anotherAction = req -> {
			ByteBuf msg = wrapUtf8("Shall not be executed: " + req.getPath());
			return HttpResponse.ofCode(200).withBody(msg).toPromise();
		};

		RoutingServlet main;
		try {
			Reactor reactor = getCurrentReactor();
			main = RoutingServlet.builder(reactor)
				.with(GET, "/", action)
				.with(GET, "/a/e", action)
				.with(GET, "/a/c/f", action)
				.with(GET, "/", RoutingServlet.builder(reactor)
					.with(GET, "/a/c/f", anotherAction)
					.build())
				.build();
		} catch (IllegalArgumentException e) {
			assertEquals("Already mapped", e.getMessage());
			return;
		}

		check(main.serve(request), "SHALL NOT BE EXECUTED", 500);
	}

	@Test
	public void testParameter() throws Exception {
		AsyncServlet printParameters = request -> {
			String body =
				request.getPathParameter("id") +
				" " +
				request.getPathParameter("uid");
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.ofCode(200).withBody(bodyByteBuf).toPromise();
		};

		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/:id/a/:uid/b/:eid", printParameters)
			.with(GET, "/:id/a/:uid", printParameters)
			.build();

		System.out.println("Parameter test " + DELIM);
		check(main.serve(HttpRequest.get("http://example.com/123/a/456/b/789").build()), "123 456", 200);
		check(main.serve(HttpRequest.get("http://example.com/555/a/777").build()), "555 777", 200);
		check(main.serve(HttpRequest.get("http://example.com").build()), "", 404);
		System.out.println();
	}

	@Test
	public void testMultiParameters() throws Exception {
		RoutingServlet ms = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/serve/:cid/wash", request -> {
				ByteBuf body = wrapUtf8("served car: " + request.getPathParameter("cid"));
				return HttpResponse.ofCode(200).withBody(body).toPromise();
			})
			.with(GET, "/serve/:mid/feed", request -> {
				ByteBuf body = wrapUtf8("served man: " + request.getPathParameter("mid"));
				return HttpResponse.ofCode(200).withBody(body).toPromise();
			})
			.build();

		System.out.println("Multi parameters " + DELIM);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/1/wash").build()), "served car: 1", 200);
		check(ms.serve(HttpRequest.get(TEMPLATE + "/serve/2/feed").build()), "served man: 2", 200);
		System.out.println();
	}

	@Test
	public void testDifferentMethods() throws Exception {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/a/b/c/action").build();
		HttpRequest request2 = HttpRequest.post(TEMPLATE + "/a/b/c/action").build();
		HttpRequest request3 = HttpRequest.builder(CONNECT, TEMPLATE + "/a/b/c/action").build();

		RoutingServlet servlet = RoutingServlet.builder(getCurrentReactor())
			.with("/a/b/c/action", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("WILDCARD")).toPromise())
			.with(POST, "/a/b/c/action", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("POST")).toPromise())
			.with(GET, "/a/b/c/action", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("GET")).toPromise())
			.build();

		System.out.println("Different methods " + DELIM);
		check(servlet.serve(request1), "GET", 200);
		check(servlet.serve(request2), "POST", 200);
		check(servlet.serve(request3), "WILDCARD", 200);
		System.out.println();
	}

	@Test
	public void testDefault() throws Exception {
		HttpRequest request1 = HttpRequest.get(TEMPLATE + "/html/admin/action").build();
		HttpRequest request2 = HttpRequest.get(TEMPLATE + "/html/admin/action/ban").build();

		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/html/admin/action", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("Action executed")).toPromise())
			.with("/html/admin/*", request ->
				HttpResponse.ofCode(200)
					.withBody(wrapUtf8("Stopped at admin: " + request.getRelativePath()))
					.toPromise())
			.build();

		System.out.println("Default stop " + DELIM);
		check(main.serve(request1), "Action executed", 200);
		check(main.serve(request2), "Stopped at admin: action/ban", 200);
		System.out.println();
	}

	@Test
	public void test404() throws Exception {
		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with("/a/:id/b/d", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("All OK")).toPromise())
			.build();

		System.out.println("404 " + DELIM);
		HttpRequest request = HttpRequest.get(TEMPLATE + "/a/123/b/c").build();
		check(main.serve(request), "", 404);
		System.out.println();
	}

	@Test
	public void test405() throws Exception {
		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/a/:id/b/d", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("Should not execute")).toPromise())
			.build();

		HttpRequest request = HttpRequest.post(TEMPLATE + "/a/123/b/d").build();
		check(main.serve(request), "", 404);
	}

	@Test
	public void test405WithFallback() throws Exception {
		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/a/:id/b/d", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("Should not execute")).toPromise())
			.with("/a/:id/b/d", request ->
				HttpResponse.ofCode(200).withBody(wrapUtf8("Fallback executed")).toPromise())
			.build();
		check(main.serve(HttpRequest.post(TEMPLATE + "/a/123/b/d").build()), "Fallback executed", 200);
	}

	@Test
	public void testTail() throws Exception {
		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/method/:var/*", request -> {
				ByteBuf body = wrapUtf8("Success: " + request.getRelativePath());
				return HttpResponse.ofCode(200).withBody(body).toPromise();
			})
			.build();

		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dfbdb/oneArg").build()), "Success: oneArg", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/srfethj/first/second").build()), "Success: first/second", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/dvyhju/").build()), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn").build()), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn?query=string").build()), "Success: ", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + "/method/yumgn/first?query=string").build()), "Success: first", 200);
	}

	@Test
	public void testWebSocket() throws Exception {
		String wsPath = "/web/socket";
		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.withWebSocket(wsPath, request -> {})
			.with(POST, wsPath, request -> HttpResponse.ok200().withBody(wrapUtf8("post")).toPromise())
			.with(GET, wsPath, request -> HttpResponse.ok200().withBody(wrapUtf8("get")).toPromise())
			.with(wsPath, request -> HttpResponse.ok200().withBody(wrapUtf8("all")).toPromise())
			.with(wsPath + "/inside", request -> HttpResponse.ok200().withBody(wrapUtf8("inner")).toPromise())
			.build();

		checkWebSocket(main.serve(HttpRequest.get(TEMPLATE_WS + wsPath).build()));
		check(main.serve(HttpRequest.post(TEMPLATE + wsPath).build()), "post", 200);
		check(main.serve(HttpRequest.get(TEMPLATE + wsPath).build()), "get", 200);
		check(main.serve(HttpRequest.builder(OPTIONS, TEMPLATE + wsPath).build()), "all", 200);
		check(main.serve(HttpRequest.builder(OPTIONS, TEMPLATE + wsPath + "/inside").build()), "inner", 200);
	}

	@Test
	public void testWebSocketSingle() throws Exception {
		String wsPath = "/web/socket";
		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.withWebSocket(wsPath, request -> HttpResponse.ok200().build())
			.build();

		checkWebSocket(main.serve(HttpRequest.get(TEMPLATE_WS + wsPath).build()));
		check(main.serve(HttpRequest.get(TEMPLATE + wsPath).build()), "", 404);
		check(main.serve(HttpRequest.post(TEMPLATE + wsPath).build()), "", 404);
	}

	@Test
	public void testPercentEncoding() throws Exception {
		AsyncServlet servlet = request -> HttpResponse.ofCode(200).withBody("".getBytes(UTF_8)).toPromise();

		RoutingServlet router = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/a%2fb", servlet)
			.initialize(builder -> {
				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> builder.with(GET, "/a%2Fb", servlet));
				assertEquals("Already mapped", e.getMessage());
			})
			.build();

		check(router.serve(HttpRequest.get("http://some-test.com/a%2fb").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/a%2Fb").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/a/b").build()), "", 404);
	}

	@Test
	public void testPercentEncodingCyrillic() throws Exception {
		AsyncServlet servlet = request -> HttpResponse.ofCode(200).withBody("".getBytes(UTF_8)).toPromise();

		RoutingServlet router = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/абв", servlet)
			.initialize(builder -> {
				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> builder.with(GET, "/%D0%B0%D0%B1%D0%B2", servlet));
				assertEquals("Already mapped", e.getMessage());
			})
			.build();

		check(router.serve(HttpRequest.get("http://some-test.com/%d0%b0%d0%b1%d0%b2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%b0%d0%b1%d0%b2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%d0%b1%d0%b2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%b1%d0%b2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%B1%d0%b2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%B1%D0%b2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/%D0%B0%D0%B1%D0%B2").build()), "", 200);
		check(router.serve(HttpRequest.get("http://some-test.com/абв").build()), "", 404);
	}

	@Test
	public void testPercentEncodedParameters() throws Exception {
		AsyncServlet printParameters = request -> {
			String body = new TreeMap<>(request.getPathParameters()).toString();
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.ofCode(200).withBody(bodyByteBuf).toPromise();
		};

		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/a/:val", printParameters)
			.build();

		check(main.serve(HttpRequest.get("http://example.com/a/1%2f").build()), "{val=1/}", 200);
		check(main.serve(HttpRequest.get("http://example.com/a/1%2F").build()), "{val=1/}", 200);
		check(main.serve(HttpRequest.get("http://example.com/a/1+").build()), "{val=1 }", 200);
		check(main.serve(HttpRequest.get("http://example.com/a/1%252f").build()), "{val=1%2f}", 200);
	}

	@Test
	public void testPercentEncodedParameterName() throws Exception {
		AsyncServlet printParameters = request -> {
			String body = new TreeMap<>(request.getPathParameters()).toString();
			ByteBuf bodyByteBuf = wrapUtf8(body);
			return HttpResponse.ofCode(200).withBody(bodyByteBuf).toPromise();
		};

		RoutingServlet main = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/a/:%2f", printParameters)
			.build();

		check(main.serve(HttpRequest.get("http://example.com/a/23").build()), "{%2f=23}", 200);
	}

	@Test
	public void testBadPercentEncoding() throws Exception {
		AsyncServlet servlet = request -> HttpResponse.ofCode(200).withBody(new byte[0]).toPromise();
		RoutingServlet router = RoutingServlet.builder(getCurrentReactor())
			.with(GET, "/a", servlet)
			.initialize(builder -> {
				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> builder.with(GET, "/a%2", servlet));
				assertEquals("Pattern contains bad percent encoding", e.getMessage());
			})
			.build();

		HttpError e = assertThrows(HttpError.class, () -> router.serve(HttpRequest.get("http://example.com/a%2").build()));
		assertEquals("HTTP code 400: Path contains bad percent encoding", e.getMessage());
	}
}
