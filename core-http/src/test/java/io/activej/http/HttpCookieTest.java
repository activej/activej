package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.Utils.first;
import static java.time.Month.JANUARY;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.*;

public class HttpCookieTest {
	@ClassRule
	public static final ByteBufRule rule = new ByteBufRule();

	@Test
	public void testDecoder() throws MalformedHttpException {
		String cookieString = "name1=\"value1\"; expires=Thu, 01 Jan 2015 00:00:00 GMT; Secure; name2=value2; HttpOnly";
		List<HttpCookie> httpCookies = new ArrayList<>();
		byte[] bytes = encodeAscii(cookieString);
		HttpCookie.decodeFull(bytes, 0, bytes.length, httpCookies);
		assertEquals(2, httpCookies.size());
		HttpCookie cookie1 = httpCookies.get(0);
		HttpCookie cookie2 = httpCookies.get(1);

		assertEquals("name1", cookie1.getName());
		assertEquals("value1", cookie1.getValue());
		assertEquals(cookie1.getExpirationDate(), LocalDate.of(2015, JANUARY, 1).atStartOfDay().toInstant(UTC));
		assertEquals(-1, cookie1.getMaxAge());
		assertTrue(cookie1.isSecure());
		assertFalse(cookie1.isHttpOnly());

		assertEquals("name2", cookie2.getName());
		assertEquals("value2", cookie2.getValue());
		assertFalse(cookie2.isSecure());
		assertTrue(cookie2.isHttpOnly());
	}

	@Test
	public void testRender() {
		Instant date = Instant.ofEpochMilli(987654321098L); // "Thu, 19 Apr 2001 04:25:21 GMT";
		HttpCookie cookie = HttpCookie.of("name", "value")
				.withExpirationDate(date)
				.withMaxAge(Duration.ofSeconds(10))
				.withPath("/test")
				.withDomain("www.google.com")
				.withSecure(true)
				.withHttpOnly(true)
				.withExtension("Alhambra site");

		String expected = "name=value; Expires=Thu, 19 Apr 2001 04:25:21 GMT; Max-Age=10; Domain=www.google.com; " +
				"Path=/test; Secure; HttpOnly; Alhambra site";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		cookie.renderFull(buf);
		assertEquals(expected, ByteBufStrings.asAscii(buf));
	}

	@Test
	public void testRenderMany() {
		Instant date = Instant.ofEpochMilli(987654321098L); // "Thu, 19 Apr 2001 04:25:21 GMT";
		HttpCookie cookie1 = HttpCookie.of("name1", "value1")
				.withExpirationDate(date)
				.withMaxAge(Duration.ofSeconds(10))
				.withPath("/test")
				.withDomain("www.google.com")
				.withSecure(true);

		HttpCookie cookie2 = HttpCookie.of("name2", "value2")
				.withHttpOnly(true)
				.withExtension("Alhambra site");
		HttpCookie cookie3 = HttpCookie.of("name3");

		String expected = "name1=value1; name2=value2; name3";

		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		HttpCookie.renderSimple(List.of(cookie1, cookie2, cookie3), buf);
		assertEquals(expected, ByteBufStrings.asAscii(buf));
	}

	@Test
	public void testDecode() throws MalformedHttpException {
		String cookieName = "HMECOMDIC";
		String cookieValue = "{\"osVersion\":\"x86_64\",\"deviceOs\":\"Linux\",\"deviceType\":\"DESKTOP\"}";
		byte[] bytes = ByteBufStrings.encodeAscii(cookieName + "=" + cookieValue);

		ArrayList<HttpCookie> cookies = new ArrayList<>();
		HttpCookie.decodeFull(bytes, 0, bytes.length, cookies);

		assertEquals(1, cookies.size());

		HttpCookie cookie = first(cookies);

		assertEquals(cookieName, cookie.getName());
		assertEquals(cookieValue, cookie.getValue());
	}

	@Test
	public void testRenderPathSlash() {
		HttpCookie cookie = HttpCookie.of("name", "value")
				.withPath("/");

		String expected = "name=value; Path=/";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		cookie.renderFull(buf);
		assertEquals(expected, ByteBufStrings.asAscii(buf));
	}

	@Test
	public void testDecodePathSlash() throws MalformedHttpException {
		String cookieName = "name";
		String cookieValue = "value";
		String cookiePath = "/";
		byte[] bytes = ByteBufStrings.encodeAscii(cookieName + "=" + cookieValue + "; Path=" + cookiePath);

		ArrayList<HttpCookie> cookies = new ArrayList<>();
		HttpCookie.decodeFull(bytes, 0, bytes.length, cookies);

		assertEquals(1, cookies.size());

		HttpCookie cookie = first(cookies);

		assertEquals(cookieName, cookie.getName());
		assertEquals(cookieValue, cookie.getValue());
		assertEquals(cookiePath, cookie.getPath());
	}

	@Test
	public void testRenderPathEmpty() {
		HttpCookie cookie = HttpCookie.of("name", "value");

		String expected = "name=value";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		cookie.renderFull(buf);
		assertEquals(expected, ByteBufStrings.asAscii(buf));
	}

	@Test
	public void testDecodePathEmpty() throws MalformedHttpException {
		String cookieName = "name";
		String cookieValue = "value";
		byte[] bytes = ByteBufStrings.encodeAscii(cookieName + "=" + cookieValue);

		ArrayList<HttpCookie> cookies = new ArrayList<>();
		HttpCookie.decodeFull(bytes, 0, bytes.length, cookies);

		assertEquals(1, cookies.size());

		HttpCookie cookie = first(cookies);

		assertEquals(cookieName, cookie.getName());
		assertEquals(cookieValue, cookie.getValue());
		assertEquals("/", cookie.getPath());
	}

	@Test
	public void testCommaDelimiter() {
		HttpResponse response = HttpResponse.ofCode(200);
		response.addCookies(List.of(HttpCookie.of("key1", "value1"), HttpCookie.of("key2", "value2")));
		assertEquals(2, response.getCookies().size());
	}
}
