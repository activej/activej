package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;

import static io.activej.http.HttpHeaders.HOST;
import static io.activej.http.HttpHeaders.of;
import static io.activej.http.HttpMethod.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class HttpMessageTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static void assertHttpMessageEquals(String expected, HttpMessage message) {
		ByteBuf buf = AbstractHttpConnection.renderHttpMessage(message);
		assertNotNull(buf);
		String actual = ByteBufStrings.asAscii(buf);

		assertEquals(new LinkedHashSet<>(List.of(expected.split("\r\n"))), new LinkedHashSet<>(List.of(actual.split("\r\n"))));
		message.recycle();
	}

	@Test
	public void testHttpResponse() {
		assertHttpMessageEquals("""
				HTTP/1.1 100 Continue\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(100));
		assertHttpMessageEquals("""
				HTTP/1.1 123 OK\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(123));
		assertHttpMessageEquals("""
				HTTP/1.1 200 OK\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(200));
		assertHttpMessageEquals("""
				HTTP/1.1 400 Bad Request\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(400));
		assertHttpMessageEquals("""
				HTTP/1.1 405 Method Not Allowed\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(405));
		assertHttpMessageEquals("""
				HTTP/1.1 456 Error\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(456));
		assertHttpMessageEquals("""
				HTTP/1.1 500 Internal Server Error\r
				Content-Length: 0\r
				\r
				""", HttpResponse.ofCode(500));
		assertHttpMessageEquals("""
				HTTP/1.1 502 Bad Gateway\r
				Content-Length: 11\r
				\r
				Bad Gateway""", HttpResponse.ofCode(502).withBody("Bad Gateway".getBytes(StandardCharsets.UTF_8)));
		assertHttpMessageEquals("""
						HTTP/1.1 200 OK\r
						Set-Cookie: cookie1=value1\r
						Content-Length: 0\r
						\r
						""",
				HttpResponse.ofCode(200).withCookies(List.of(HttpCookie.of("cookie1", "value1"))));
		assertHttpMessageEquals("""
						HTTP/1.1 200 OK\r
						Set-Cookie: cookie1=value1\r
						Set-Cookie: cookie2=value2\r
						Content-Length: 0\r
						\r
						""",
				HttpResponse.ofCode(200).withCookies(List.of(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));
		assertHttpMessageEquals("""
						HTTP/1.1 200 OK\r
						Set-Cookie: cookie1=value1\r
						Set-Cookie: cookie2=value2\r
						Content-Length: 0\r
						\r
						""",
				HttpResponse.ofCode(200).withCookies(List.of(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));
	}

	@Test
	public void testHttpRequest() {
		assertHttpMessageEquals("""
				GET /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.get("http://test.com/index.html"));
		assertHttpMessageEquals("""
				POST /index.html HTTP/1.1\r
				Host: test.com\r
				Content-Length: 0\r
				\r
				""", HttpRequest.post("http://test.com/index.html"));
		assertHttpMessageEquals("""
				CONNECT /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.of(HttpMethod.CONNECT, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				GET /index.html HTTP/1.1\r
				Host: test.com\r
				Cookie: cookie1=value1\r
				\r
				""", HttpRequest.get("http://test.com/index.html").withCookie(HttpCookie.of("cookie1", "value1")));
		assertHttpMessageEquals("""
				GET /index.html HTTP/1.1\r
				Host: test.com\r
				Cookie: cookie1=value1; cookie2=value2\r
				\r
				""", HttpRequest.get("http://test.com/index.html").withCookies(List.of(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));

		HttpRequest request = HttpRequest.post("http://test.com/index.html");
		ByteBuf buf = ByteBufPool.allocate(100);
		buf.put("/abc".getBytes(), 0, 4);
		request.setBody(buf);
		assertHttpMessageEquals("""
				POST /index.html HTTP/1.1\r
				Host: test.com\r
				Content-Length: 4\r
				\r
				/abc""", request);
	}

	@Test
	public void testHttpRequestWithNoPayload() {
		assertHttpMessageEquals("""
				GET /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.of(GET, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				HEAD /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.of(HEAD, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				CONNECT /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.of(CONNECT, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				OPTIONS /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.of(OPTIONS, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				TRACE /index.html HTTP/1.1\r
				Host: test.com\r
				\r
				""", HttpRequest.of(TRACE, "http://test.com/index.html"));

		assertHttpMessageEquals("""
				POST /index.html HTTP/1.1\r
				Host: test.com\r
				Content-Length: 0\r
				\r
				""", HttpRequest.of(POST, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				PUT /index.html HTTP/1.1\r
				Host: test.com\r
				Content-Length: 0\r
				\r
				""", HttpRequest.of(PUT, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				DELETE /index.html HTTP/1.1\r
				Host: test.com\r
				Content-Length: 0\r
				\r
				""", HttpRequest.of(DELETE, "http://test.com/index.html"));
		assertHttpMessageEquals("""
				PATCH /index.html HTTP/1.1\r
				Host: test.com\r
				Content-Length: 0\r
				\r
				""", HttpRequest.of(PATCH, "http://test.com/index.html"));
	}

	@Test
	public void testMultiHeaders() {
		HttpResponse response = HttpResponse.ofCode(200);
		HttpHeader header1 = of("header1");
		HttpHeader HEADER1 = of("HEADER1");

		response.addHeader(header1, "value1");
		response.addHeader(HEADER1, "VALUE1");
		assertHttpMessageEquals("""
				HTTP/1.1 200 OK\r
				header1: value1\r
				HEADER1: VALUE1\r
				Content-Length: 0\r
				\r
				""", response);
	}

	@Test
	public void testFullUrlOnClient() {
		String url = "http://example.com/a/b/c/d?param1=test1&param2=test2#fragment";

		assertEquals(url, HttpRequest.get(url).getFullUrl());
	}

	@Test
	public void testFullUrlOnServer() throws MalformedHttpException {
		String host = "example.com";
		String url = "/a/b/c/d?param1=test1&param2=test2#fragment";
		String expected = "https://example.com/a/b/c/d?param1=test1&param2=test2#fragment";

		HttpRequest request = new HttpRequest(HttpVersion.HTTP_1_1, GET, UrlParser.parse(url), null);
		request.addHeader(HOST, host);
		request.setProtocol(Protocol.HTTPS);

		assertEquals(expected, request.getFullUrl());
	}
}
