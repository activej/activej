package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.test.rules.ByteBufRule;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static io.activej.http.Protocol.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public final class HttpUrlTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testSimple() {
		UrlParser url = UrlParser.of("https://127.0.0.1:45678");

		assertSame(HTTPS, url.getProtocol());
		assertEquals("127.0.0.1", url.getHost());
		assertEquals(45678, url.getPort());
	}

	@Test
	public void testWebSocket() {
		UrlParser url = UrlParser.of("ws://127.0.0.1:45678");

		assertSame(WS, url.getProtocol());
		assertEquals("127.0.0.1", url.getHost());
		assertEquals(45678, url.getPort());
	}

	@Test
	public void testSecureWebSocket() {
		UrlParser url = UrlParser.of("wss://127.0.0.1:45678");

		assertSame(WSS, url.getProtocol());
		assertEquals("127.0.0.1", url.getHost());
		assertEquals(45678, url.getPort());
	}

	@Test
	public void testIPv6() {
		// with port
		UrlParser url = UrlParser.of("http://[0:0:0:0:0:0:0:1]:52142");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]:52142", url.getHostAndPort());
		assertEquals(52142, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		// without port
		url = UrlParser.of("http://[0:0:0:0:0:0:0:1]");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHostAndPort());
		assertEquals(80, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		// with query
		url = UrlParser.of("http://[0:0:0:0:0:0:0:1]:52142/path1/path2?aa=bb&zz=a+b");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]:52142", url.getHostAndPort());
		assertEquals(52142, url.getPort());
		assertEquals("/path1/path2?aa=bb&zz=a+b", url.getPathAndQuery());
		assertEquals("/path1/path2", url.getPath());
		assertEquals("aa=bb&zz=a+b", url.getQuery());

		url = UrlParser.of("http://[0:0:0:0:0:0:0:1]/?");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHostAndPort());
		assertEquals(80, url.getPort());
		assertEquals("/?", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());
	}

	@Test
	public void testFullUrl() {
		UrlParser url = UrlParser.of("http://abc.com");
		assertFalse(url.isRelativePath());
		assertEquals("abc.com", url.getHostAndPort());
		assertEquals("abc.com", url.getHost());
		assertEquals(80, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		url = UrlParser.of("http://zzz.abc.com:8080/path1/path2?aa=bb&zz=a+b");
		assertFalse(url.isRelativePath());
		assertEquals("zzz.abc.com:8080", url.getHostAndPort());
		assertEquals("zzz.abc.com", url.getHost());
		assertEquals(8080, url.getPort());
		assertEquals("/path1/path2?aa=bb&zz=a+b", url.getPathAndQuery());
		assertEquals("/path1/path2", url.getPath());
		assertEquals("aa=bb&zz=a+b", url.getQuery());

		url = UrlParser.of("http://zzz.abc.com/?");
		assertFalse(url.isRelativePath());
		assertEquals("zzz.abc.com", url.getHostAndPort());
		assertEquals("zzz.abc.com", url.getHost());
		assertEquals(80, url.getPort());
		assertEquals("/?", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());
	}

	@Test
	public void testPartialUrl() throws MalformedHttpException {
		UrlParser url = UrlParser.parse("/path1/path2?aa=bb&zz=a+b");
		assertTrue(url.isRelativePath());
		assertNull(url.getHostAndPort());
		assertNull(url.getHost());
		assertEquals(-1, url.getPort());
		assertEquals("/path1/path2?aa=bb&zz=a+b", url.getPathAndQuery());
		assertEquals(25, url.getPathAndQueryLength());
		assertEquals("/path1/path2", url.getPath());
		assertEquals("aa=bb&zz=a+b", url.getQuery());

		url = UrlParser.parse("");
		assertTrue(url.isRelativePath());
		assertNull(url.getHostAndPort());
		assertNull(url.getHost());
		assertEquals(-1, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());
	}

	@Test(expected = MalformedHttpException.class)
	public void testInvalidScheme() throws MalformedHttpException {
		UrlParser.parse("ftp://abc.com/");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidPartialUrl() {
		//noinspection ResultOfMethodCallIgnored
		UrlParser.of("/path").isRelativePath();
	}

	@Test(expected = MalformedHttpException.class)
	public void testBadPort() throws MalformedHttpException {
		UrlParser.parse("http://hello-world.com:80ab/path");
	}

	@Test
	public void testPollUrlPartBase() {
		UrlParser uri = UrlParser.of("http://example.com/a/b/c/index.html");
		assertEquals("a", uri.pollUrlPart());
		assertEquals("/b/c/index.html", uri.getPartialPath());
		assertEquals("b", uri.pollUrlPart());
		assertEquals("/c/index.html", uri.getPartialPath());
		assertEquals("c", uri.pollUrlPart());
		assertEquals("index.html", uri.pollUrlPart());
		assertEquals("", uri.pollUrlPart());
	}

	@Test
	public void testPollUrlPartWithPercentEncoding() {
		UrlParser uri = UrlParser.of("http://example.com/a%2f/b%2F");
		assertEquals("a/", uri.pollUrlPart());
		assertEquals("/b%2F", uri.getPartialPath());
		assertEquals("b/", uri.pollUrlPart());
		assertEquals("", uri.pollUrlPart());
	}

	@Test
	public void testPollUrlPartWithBadPercentEncoding() {
		UrlParser uri = UrlParser.of("http://example.com/a%2");
		assertNull("a/", uri.pollUrlPart());
	}

	@Test
	public void testPollUrlPartWithNotUrlEncodedQuery() throws MalformedHttpException {
		UrlParser url = UrlParser.parse("/category/url?url=http://example.com");
		assertEquals("category", url.pollUrlPart());
		assertEquals("url", url.pollUrlPart());
		assertEquals("", url.pollUrlPart());

		url = UrlParser.parse("/category/url/?url=http://example.com");
		assertEquals("category", url.pollUrlPart());
		assertEquals("url", url.pollUrlPart());
		assertEquals("", url.pollUrlPart());
	}

	@Test
	public void testGetPartialPathInUrlWithEmptyPath() {
		UrlParser urlWoSlash = UrlParser.of("https://127.0.0.1:45678");
		UrlParser urlWSlash = UrlParser.of("https://127.0.0.1:45678/");
		assertEquals("/", urlWoSlash.getPartialPath());
		assertEquals("/", urlWSlash.getPartialPath());
		assertEquals("", urlWoSlash.pollUrlPart());
		assertEquals("", urlWSlash.pollUrlPart());
	}

	@Test
	public void testExoticQueries() {
		UrlParser url = UrlParser.of("http://example.com/path1/path2/?url=https://login:pass@example.com/?key1=value1%26key2=value2&ver=2.5.*;#fragment@;.*/:abc");
		assertEquals("https://login:pass@example.com/?key1=value1&key2=value2", url.getQueryParameter("url"));
		assertEquals("2.5.*;", url.getQueryParameter("ver"));
	}

	@Test
	public void testFragment() {
		UrlParser url = UrlParser.of("http://example.com/a/b/c/index.html?q=1&key=value#section-2.1");
		assertEquals("example.com", url.getHost());
		assertEquals("/a/b/c/index.html?q=1&key=value", url.getPathAndQuery());
		assertEquals("section-2.1", url.getFragment());
	}

	@Test
	public void testGetPathAndQuery() {
		UrlParser url = UrlParser.of("http://example.com/a/b/c/index.html?q=1&key=value#section-2.1");
		assertEquals("/a/b/c/index.html?q=1&key=value", url.getPathAndQuery());

		url = UrlParser.of("http://example.com/?a=a&b=b&c#abcd");
		assertEquals("/?a=a&b=b&c", url.getPathAndQuery());
	}

	@Test
	public void testGetPathAndQueryLength() {
		UrlParser url = UrlParser.of("http://example.com/a/b/c/index.html?q=1&key=value#section-2.1");
		assertEquals(31, url.getPathAndQueryLength());

		url = UrlParser.of("http://example.com/?a=a&b=b&c#abcd");
		assertEquals(11, url.getPathAndQueryLength());

		url = UrlParser.of("http://example.com/#abcd");
		assertEquals(1, url.getPathAndQueryLength());
	}

	@Test
	public void testUrlWithQmInFragment() {
		UrlParser url = UrlParser.of("http://example.com/a/b/c/index.html#section-2.1?q=1&key=value&b:c");
		assertEquals(17, url.getPathAndQueryLength());
		ByteBuf buf = ByteBufPool.allocate(64);
		url.writePathAndQuery(buf);
		assertEquals("/a/b/c/index.html", ByteBufStrings.asAscii(buf));
		assertEquals("section-2.1?q=1&key=value&b:c", url.getFragment());
	}

	@Test
	public void testWritePathAndQuery() {
		UrlParser url = UrlParser.of("http://example.com:1234/path1/path2/path3/?key1=value1&key2#sec:2.2");
		ByteBuf buf = ByteBufPool.allocate(64);
		url.writePathAndQuery(buf);

		assertEquals(0, buf.head());
		assertEquals(36, buf.tail());
		assertEquals("/path1/path2/path3/?key1=value1&key2", ByteBufStrings.asAscii(buf));

		url = UrlParser.of("http://example.com:1234?key1=value1&key2#sec:2.2");
		buf = ByteBufPool.allocate(64);
		url.writePathAndQuery(buf);

		assertEquals(0, buf.head());
		assertEquals(18, buf.tail());
		assertEquals("/?key1=value1&key2", ByteBufStrings.asAscii(buf));
	}

	@Test
	public void testUrlWithColonInPath() {
		String domain = "www.example.in";
		String path = "/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/www.example.in/search/label/javascript:void(0)";
		String query = "q=v&q";
		String fragment = "abc/a";
		String url = "http://" + domain + path + "?" + query + "#" + fragment;
		UrlParser httpUrl = UrlParser.of(url);

		assertEquals(domain, httpUrl.getHost());
		assertEquals(path, httpUrl.getPath());
		assertEquals(query, httpUrl.getQuery());
		assertEquals(fragment, httpUrl.getFragment());
	}

	@Test
	public void testQuery() {
//                                00000000001111111111222222222233333333334444444444555555555566
//  							  01234567890123456789012345678901234567890123456789012345678901
		UrlParser url = UrlParser.of("http://abc.com/?key=value&key&key=value2&key3=another_value&k");
		url.parseQueryParameters();

		assertArrayEquals(new int[]{1245200, 1900570, 2162718, 2949161, 3997756, 0, 0, 0}, url.queryPositions);

		assertEquals("value", url.getQueryParameter("key"));
		assertEquals("another_value", url.getQueryParameter("key3"));
		assertEquals("", url.getQueryParameter("k"));
		assertNull(url.getQueryParameter("missing"));

		List<String> parameters = url.getQueryParameters("key");
		assertEquals(asList("value", "", "value2"), parameters);

		Iterable<QueryParameter> queryParameters = url.getQueryParametersIterable();
		assertNotNull(queryParameters);
		Iterator<QueryParameter> paramsIterator = queryParameters.iterator();
		assertEquals(new QueryParameter("key", "value"), paramsIterator.next());
		assertEquals(new QueryParameter("key", ""), paramsIterator.next());
		assertEquals(new QueryParameter("key", "value2"), paramsIterator.next());
		assertEquals(new QueryParameter("key3", "another_value"), paramsIterator.next());
		assertEquals(new QueryParameter("k", ""), paramsIterator.next());

		try {
			paramsIterator.next();
			fail();
		} catch (NoSuchElementException ignored) {
		}
	}

	@Test
	public void testQueryWithNoPathBefore() {
		UrlParser url = UrlParser.of("http://google.com?query=one:two/something");

		assertEquals("google.com", url.getHost());
		assertEquals("/", url.getPath());
		assertEquals("query=one:two/something", url.getQuery());
	}

	private static Map<String, String> map(String[]... values) {
		return Arrays.stream(values).collect(Collectors.toMap(o -> o[0], o -> o[1]));
	}

	private static String[] entry(String key, String value) {
		return new String[]{key, value};
	}

	@Test
	public void testLastEmptyValue() {
		Map<String, String> map = map(
				entry("key1", "value"),
				entry("key2", ""),
				entry("key3", "value2"),
				entry("key4", "another_value"),
				entry("k5", ""));

		UrlParser url = UrlParser.of("http://abc.com/?key1=value&key2&key3=value2&key4=another_value&k5");
		assertEquals(map, url.getQueryParameters());
		for (String key : map.keySet()) {
			assertEquals(map.get(key), url.getQueryParameter(key));
		}
	}

	@Test
	public void testAmpersandLastCharacter() {
		Map<String, String> map = map(
				entry("key1", "value"),
				entry("key2", ""),
				entry("key3", "value2"),
				entry("key4", "another_value"));

		UrlParser url = UrlParser.of("http://abc.com/?key1=value&key2&key3=value2&key4=another_value&");
		assertEquals(map, url.getQueryParameters());
		for (String key : map.keySet()) {
			assertEquals(map.get(key), url.getQueryParameter(key));
		}
	}

	@Test
	public void testEmptyQuery() {
		UrlParser url = UrlParser.of("http://127.0.0.1/?&&");
		Set<String> actual = url.getQueryParameters().keySet();
		assertThat(actual, IsEmptyCollection.empty());
	}

	@Test
	public void testEmptyValueBeforeAmpersandWithSeparator() {
		Map<String, String> map = singletonMap("key", "");
		UrlParser url = UrlParser.of("http://abc.com/?key=&");
		assertEquals(map, url.getQueryParameters());
		for (String key : map.keySet()) {
			assertEquals(map.get(key), url.getQueryParameter(key));
		}
	}

	@Test
	public void testQuery2() {
		UrlParser url = UrlParser.of("http://www.test.com/test?a=1&&b=12+45%20%20%20&c=2&d=abc&&x#fragment");
		assertEquals("1", url.getQueryParameter("a"));
		assertEquals("12 45   ", url.getQueryParameters().get("b"));
		assertEquals(singletonList("2"), url.getQueryParameters("c"));
		assertEquals("abc", url.getQueryParameter("d"));
		assertEquals("", url.getQueryParameter("x"));
	}

	@Test
	public void testEmptyHost() {
		assertThrows(IllegalArgumentException.class, () -> UrlParser.of("http://:80/"));
	}

	@Test
	public void testEmptyHost2() {
		assertThrows(IllegalArgumentException.class, () -> UrlParser.of("http:///"));
	}

	@Test
	public void testEmptyHost3() {
		assertThrows(IllegalArgumentException.class, () -> UrlParser.of("http://?test=':80'"));
	}

	@Test
	public void testHostFollowedByFragment() {
		UrlParser url = UrlParser.of("http://www.test.com#fragment");
		assertEquals(HTTP, url.getProtocol());
		assertEquals("www.test.com", url.getHost());
		assertEquals("fragment", url.getFragment());
	}

}
