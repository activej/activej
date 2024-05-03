package io.activej.http;

import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static io.activej.http.ContentTypes.JSON_UTF_8;
import static io.activej.http.HttpHeaderValue.*;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.MediaTypes.ANY_IMAGE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class HttpHeadersTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testValuesToStrings() {
		HttpRequest request = HttpRequest.post("http://example.com")
			.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
			.withHeader(ACCEPT, ofAcceptMediaTypes(AcceptMediaType.of(ANY_IMAGE, 50), AcceptMediaType.of(MediaTypes.HTML)))
			.withHeader(ACCEPT_CHARSET, ofAcceptCharsets(AcceptCharset.of(UTF_8), AcceptCharset.of(ISO_8859_1)))
			.withCookies(HttpCookie.of("key1", "value1"), HttpCookie.of("key2"), HttpCookie.of("key3", "value2"))
			.build();

		assertEquals("application/json; charset=utf-8", request.getHeader(CONTENT_TYPE));
		assertEquals("image/*; q=0.5, text/html", request.getHeader(ACCEPT));
		assertEquals("utf-8, iso-8859-1", request.getHeader(ACCEPT_CHARSET));
		assertEquals("key1=value1; key2; key3=value2", request.getHeader(COOKIE));

		HttpResponse response = HttpResponse.ofCode(200)
			.withHeader(DATE, HttpHeaderValue.ofTimestamp(1486944000021L))
			.build();

		assertEquals("Mon, 13 Feb 2017 00:00:00 GMT", response.getHeader(DATE));
	}

	@Test
	public void testHeadersSize() {
		HttpRequest request = HttpRequest.post("http://example.com")
			.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
			.withHeader(ACCEPT, ofAcceptMediaTypes(AcceptMediaType.of(ANY_IMAGE, 50), AcceptMediaType.of(MediaTypes.HTML)))
			.withHeader(ACCEPT_CHARSET, ofAcceptCharsets(AcceptCharset.of(UTF_8), AcceptCharset.of(ISO_8859_1)))
			.build();

		Collection<Map.Entry<HttpHeader, HttpHeaderValue>> headersCollection = request.getHeaders();

		assertEquals(3, headersCollection.size());

		Map<HttpHeader, HttpHeaderValue> headersMap = headersCollection.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		assertEquals("application/json; charset=utf-8", headersMap.get(CONTENT_TYPE).toString());
		assertEquals("image/*; q=0.5, text/html", headersMap.get(ACCEPT).toString());
		assertEquals("utf-8, iso-8859-1", headersMap.get(ACCEPT_CHARSET).toString());
	}
}
